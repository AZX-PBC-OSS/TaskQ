# Agent loops: the five durable patterns

> **Status: CURRENT** (was: the agent-loop parity lane's DRAFT — T30 and
> T31 landed since, the sections rewritten to the landed faces). The
> pattern mapping, the agent-builder's guide, and the honest where-we-lack
> list, written against the landed `feat/taskqflow` surface and the
> frameworks' late-2026 docs (the receipts at the foot of the page). It is
> evidence for the adoption argument, not the argument itself.

The cowork-agent story is the target: agents that run long, wait on humans,
spawn sub-investigations, stream progress, and survive deploys and crashes.
Every framework an agent-builder might arrive from — LangGraph, the OpenAI
Agents SDK, LlamaIndex Workflows, Inngest, Temporal, Trigger.dev, CrewAI —
converged on the same five durable patterns. This page renders each pattern IN
TaskQflow's idiom, with a verified fence (every ```python fence here runs under
the docs-example harness, `TASKQ_DOCS_EXAMPLES=1` — a private schema, the real
package, no fakes), and names where our shape is better, where it is worse,
and where we lack.

The one-sentence orientation: **an agent loop is a workflow whose steps are
LLM calls and tool calls, whose waits are rows, and whose progress is a
stream an operator can replay** — TaskQflow has four and a half of the five;
the tickets at the end name the missing half.

---

## The pattern mapping (the honest table)

| # | The pattern | Their face (the arrival point) | TaskQflow's landed face | The verdict |
| --- | --- | --- | --- | --- |
| 1 | **The LLM call is a step** | Inngest `step.run("think")` (memoized, retried per step); Temporal the activity-per-LLM-call (the retry policy rides the activity); Trigger.dev `chat` (every turn a span) | `step(llm_body, ..., rate_limits=[TokenBucket(...)], max_attempts=4, cache=True)` — the retry ladder, the ADMISSION cadence cap, and the cross-run cache (T25: same body + input → the same answer without re-billing tokens) | **Parity, one better face** (the cross-run cache + the rate-limit terms stamped on the ROW), one worse (no token accounting on the step face — the spend is the body's problem) |
| 2 | **The human-approval interrupt** | LangGraph `interrupt(value)` + `Command(resume=...)` (the checkpointer holds the thread); OpenAI `needs_approval` + `RunState.approve` (the state serialization is YOURS); Trigger.dev waitpoint tokens; Temporal Signals | `ctx.wait_signal((Approval,), timeout_s=...)` → the hold ROW (the payload schema declared, the context redacted before persist, the reply audited); `HitlClient.list/get/resolve` (the typed refusal, the idempotent CAS); `Expired` is a VALUE the checker forces the fail-close arm on | **Ours is better on the operator's face** (holds are rows any UI can list, resolve, audit — the frameworks hand you a state blob or a token and leave the store to your app), worse on the ecosystem's (no LangGraph Studio / Slack-in-a-box — the resolve UI is yours) |
| 3 | **The sub-investigation (spawn/join)** | LangGraph subgraphs (a compiled graph as a node); Temporal child workflows (the handle, the cancellation cascade); Inngest `step.invoke()` (sync) / `sendEvent` (async); LlamaIndex sub-workflows | **Parity both faces**: the one-flow fan-out — `map_source(plan_step, investigate_one)` + the gather join (the fan-out's children ARE queue rows; the join fires exactly once in the finalize tx) — and the LANDED cross-run spawn: `ctx.spawn_flow(name, input=…, result=G)` → the typed `ChildOk[G] \| ChildFailed \| ChildCancelled \| Expired` join, the child a REAL run (its own holds, its own run key, the retry adopts the same child) | **Parity** — the child-flows lane's cure closed the cross-run gap (T31) |
| 4 | **The streaming / progress push** | LangGraph `stream_mode="messages"` (token deltas); Trigger.dev realtime + the durable stream (resume from the last chunk); Temporal Workflow Streams (public preview, May 2026); LlamaIndex `handler.stream_events()` | `await ctx.progress(pct, message, data)` — best-effort by construction (the coalescing law: ~20 writes/s, the occurrences counter counts what coalesced), `progress_schema=` the typed door, the STATE channel (latest-wins) + the STREAM channel (the 64-row ring, replay with the named resync), `ProgressListener`/SSE over the same rows | **Worse on granularity** (coalesced latest-wins, not per-token — a token stream needs a different bound and face: T32), **better on honesty** (the progress-lie fence: the ledger's terminal state is never overwritten by a 99% message) |
| 5 | **The durable wait / timer** | Temporal `sleep()`/`schedule_to_close` (the timer is an event in history); Inngest `step.sleep()` / `waitForEvent` (months); Trigger.dev `wait.for()` / waitpoint timeouts | **Both faces landed**: the general timer — `ctx.wait_until(when, key=…)` (the clock is the hold's resolver, the reserved `__timer__` prefix, the due-drain's partial index, the wake's receipt) — and the hold's `timeout_s=` (the expiry sweep abandons it, the wait site RETURNS `Expired` — the fail-close VALUE). The held representation: the worker releases the slot — **no sleeping worker** | **Better on the slot law** (a held node is zero occupancy — the queue's capacity is back); **at parity on generality** — the timers lane's cure closed the plain-sleep gap (T30); the wait-until-CONDITION form (the poll-shaped predicate) stays a recorded-not-built row |
| 6 | **The state checkpointing** | LangGraph: the checkpointer saves the WHOLE state blob every superstep; `get_state(history)` + replay = time travel; CrewAI checkpointing (replay from a step, fork) | The rows ARE the state: the node row (status, attempt, claim epoch), the step ledger (`wf_step_ledger` — the exactly-once witness), the decoded payloads on the edges, the run-key arbiter (T-G2: the concurrent dedup with the typed claim verdict) | **Better on queryability** (an operator SQLs the run; LangGraph's blob is opaque to the operator), **worse on the declare-and-forget simplicity** (LangGraph never asks you to think about what survives — and gets time travel free; ours is explicit steps, no fork-from-history API: T34) |
| 7 | **The tool-call idempotency** | Inngest `step.run()` memoization; Temporal the activity ID + the retry policy; Trigger.dev idempotency keys on waits | `ctx.substep(name, fn, ...)` — the step-ledger's exactly-once rows (replay returns the recorded result); the run-key arbiter for the CONCURRENT dedup; the cross-run cache for the LATER dedup | **Parity** — three dedup layers where they have one; the cost is the discipline (the body must ledger its side effects, the framework will not guess) |

---

## Pattern 1 — The LLM call is a step

The frameworks' law (Inngest's docs say it plainest): *any AI agent is really
just a series of steps. An LLM call is a step. A tool execution is a step.*
The durability comes from the step boundary, not the model. TaskQflow's idiom
is the same law with three terms the wiring call carries: the retry ladder
(`max_attempts=` — a 429 retries, cheaply, without re-running the plan), the
admission cadence (`rate_limits=` — the fleet's tokens/min as a TokenBucket the
claim path acquires; a denial snoozes the row budget-free), and the cross-run
cache (`cache=True` — the same body + the same input serves a LATER run from
the stored answer, the receipt stamped on the node row; the tokens were
billed ONCE).

```python
import asyncio
import os

import asyncpg
from pydantic import BaseModel

import taskq.migrate
from taskq.ratelimit.token_bucket import TokenBucket
from taskq.workflows import (
    Promise,
    StepContext,
    WorkflowApp,
    build,
    run,
    step,
)


class Question(BaseModel):
    q: str


class Draft(BaseModel):
    answer: str


# The fleet's cadence cap: the claim path acquires the bucket; a denial
# snoozes the row budget-free (the queue-concurrency fence's own path).
llm_bucket = TokenBucket("agent-llm", capacity=10, refill_per_second=5.0)

draft_calls = 0  # the body's invocation counter — the hit-proof's witness


async def draft_body(ctx: StepContext, question: Question) -> Draft:
    global draft_calls
    draft_calls += 1
    # THE LLM CALL LIVES HERE — the fake stands in for the client; the
    # step boundary is what makes it durable, not the client.
    return Draft(answer=f"a synthesized answer to: {question.q}")


app = WorkflowApp()


@app.workflow("agent_draft")
def agent_draft() -> Promise[object]:
    return build(
        step(
            draft_body,
            Question(q="what survived the deploy?"),
            key="draft",
            rate_limits=[llm_bucket],
            max_attempts=4,
            cache=True,  # the cross-run cache (T25): terminal-SUCCEEDED stores only
            cache_ttl=3600.0,
        )
    )


async def main() -> None:
    dsn = os.environ["TASKQ_PG_DSN"]
    schema = os.environ["TASKQ_SCHEMA_NAME"]
    await taskq.migrate.apply_pending_locked(dsn, schema=schema, phase="pre")
    await taskq.migrate.apply_pending_locked(dsn, schema=schema, phase="post")
    pool = await asyncpg.create_pool(dsn)

    first = await run(app.get("agent_draft"), pool, schema)
    calls_after_first = draft_calls
    second = await run(app.get("agent_draft"), pool, schema)
    print(
        f"first={first.outcome} calls_after_first={calls_after_first} "
        f"second={second.outcome} calls_after_second={draft_calls} "
        f"answer={second.result}"
    )


asyncio.run(main())
```

Verified output (this draft's capture, on a fresh schema):

```
first=terminal calls_after_first=1 second=terminal calls_after_second=1 answer={'answer': "a synthesized answer to: what survived the deploy?"}
```

The second run's body never executed (`calls_after_second=1`): the cache hit
delivered the first run's stored answer through the ordinary finalize — the
downstream decoded the same typed shape. The determinism hazard is stated
plainly on the wiring verb: `cache=True` promises "same body, same input",
never "the world stood still".

## Pattern 2 — The approval hold: humans are rows

The frameworks' HITL faces differ in WHERE the waiting state lives, and the
difference is the adoption cost:

- **LangGraph** pauses inside `interrupt(value)` and the CHECKPOINTER holds
  the thread; you resume with `Command(resume=...)` and a `thread_id`. The
  store is a checkpointer you configure (InMemory for demos, Postgres for
  production); the pending question is inside a serialized state blob.
- **OpenAI Agents SDK** hands you a serializable `RunState` and a
  `ToolApprovalItem`; the SDK's own docs leave the store, the routing to a
  human, and the audit trail to your application.
- **Trigger.dev** mints a waitpoint TOKEN; completing it resumes the run.
- **TaskQflow** mints a HOLD ROW — the payload schema declared at the wait
  site, the context redacted BEFORE persist, the reply validated through the
  declared models at the runtime boundary (a wrong shape is the typed
  `refused` and the hold SURVIVES), the resolution audited as a row, the
  resolve idempotent (an already-resolved hold is the defined no-op).

The agent-builder's idiom — the tuple form is the typed wait, the return is
the CLOSED union joined with `Expired`, and the checker forces the fail-close
arm:

```python
import asyncio
import os

import asyncpg
from pydantic import BaseModel

import taskq.migrate
from taskq.workflows import (
    Expired,
    FlowRunner,
    HitlClient,
    Promise,
    StepContext,
    WorkflowApp,
    build,
    step,
)


class Brief(BaseModel):
    brief: str


class Approval(BaseModel):
    approved: bool
    note: str = ""


class Shipped(BaseModel):
    verdict: str
    note: str


async def draft_brief(ctx: StepContext, params: Brief) -> Brief:
    return params  # the research is done; the brief awaits a human


async def review_gate(ctx: StepContext, brief: Brief) -> Shipped:
    outcome = await ctx.wait_signal(
        (Approval,),
        timeout_s=3600.0,  # THE RUNTIME deadline: the hold row's DB-clock expiry
        reason="the agent wants to send the brief to the client",
        tool="send_brief",
        args={"brief": brief.brief},
    )
    match outcome:
        case Approval() as approval:
            return Shipped(
                verdict="approved" if approval.approved else "rejected",
                note=approval.note,
            )
        case Expired():
            # THE FAIL-CLOSE: nobody was watching — nothing was sent.
            return Shipped(verdict="timed-out", note="nobody was watching — nothing was sent")


app = WorkflowApp()


@app.workflow("brief_ship")
def brief_ship() -> Promise[object]:
    brief = step(draft_brief, Brief(brief="the brief"), key="draft")
    return build(step(review_gate, brief, key="review"))


async def main() -> None:
    dsn = os.environ["TASKQ_PG_DSN"]
    schema = os.environ["TASKQ_SCHEMA_NAME"]
    await taskq.migrate.apply_pending_locked(dsn, schema=schema, phase="pre")
    await taskq.migrate.apply_pending_locked(dsn, schema=schema, phase="post")
    pool = await asyncpg.create_pool(dsn)

    runner = FlowRunner(app.get("brief_ship"), pool, schema)
    flow_id = (await runner.create_flow(input=Brief(brief="the brief"))).flow_id
    held = await runner.drive(flow_id, until="held")  # the worker RELEASED the slot
    hitl = HitlClient(pool, schema)
    holds = await hitl.list(run=flow_id)  # the hold: a row a UI can render
    delivery = await hitl.resolve(
        holds[0].hold_id, {"approved": True, "note": "ship it"}
    )
    await runner.drive(flow_id, until="terminal")
    final = await runner.result(flow_id)
    print(
        f"drive={held} holds={[(h.node_key, h.signal_name, h.status) for h in holds]} "
        f"delivery={delivery.status} final={final}"
    )


asyncio.run(main())
```

Verified output:

```
drive=held holds=[('review', 'Approval', 'held')] delivery=delivered final={'note': 'ship it', 'verdict': 'approved'}
```

Three operator faces ride the same rows and are not in the fence because they
are not in the process: `taskq flows holds <run_id>` (the CLI's list), the
`HitlListener` async-iterable (the typed CREATED/RESOLVED/EXPIRED/BACKFILLED
events, backfilled from the rows at start — a hold created before the listener
existed is still delivered), and the audit rows (`who resolved this` is a ROW,
not a log line). The re-execution doctrine is the honest boundary: the body
re-executes FROM THE TOP on resume — there is no determinism requirement, and
the pre-wait side effects are `ctx.substep`-ledgered and replay cheap.

## Pattern 3 — The sub-investigation: fan out, join, fire once

Anthropic's orchestrator-workers pattern (the orchestrator breaks the task
into subtasks, the workers investigate, a synthesis collects) is the
deep-research shape. The frameworks spawn: LangGraph mounts a subgraph as a
node, Temporal starts a child workflow with a handle, Inngest `invoke`s a
function. TaskQflow's landed surface expresses the pattern as the ONE-FLOW
fan-out: the orchestrator's plan is a step's returned list, `map_source` forks
one child row per item (each child its own claim, retry ladder, rate-limit
terms, progress channel), and the join's user body runs inside the join's
finalize transaction with the DECODED child results — fired exactly once.

```python
import asyncio
import os

import asyncpg
from pydantic import BaseModel

import taskq.migrate
from taskq.workflows import (
    Promise,
    StepContext,
    WorkflowApp,
    build,
    map_source,
    run,
    step,
)


class Topic(BaseModel):
    question: str
    depth: int = 1


class Finding(BaseModel):
    question: str
    sources: int


class Report(BaseModel):
    findings: list[Finding]


async def plan_body(ctx: StepContext, topics: list[Topic]) -> list[Topic]:
    # THE ORCHESTRATOR: the LLM that decomposes. Three sub-investigations.
    return topics


async def investigate_one(ctx: StepContext, topic: Topic) -> Finding:
    # THE WORKER: each child row claims, retries, and reports independently —
    # a slow investigation cannot starve its siblings' dispatch.
    await asyncio.sleep(0.05 * topic.depth)
    return Finding(question=topic.question, sources=3)


async def synthesize(ctx: StepContext, findings: list[Finding]) -> Report:
    # THE JOIN's body: decoded child results, the finalize tx — fired once.
    return Report(findings=sorted(findings, key=lambda f: f.question))


app = WorkflowApp()


@app.workflow("deep_research")
def deep_research() -> Promise[object]:
    plan = step(
        plan_body,
        [Topic(question="q1"), Topic(question="q2"), Topic(question="q3", depth=2)],
        key="plan",
    )
    investigated = map_source(plan, investigate_one)
    return build(step(synthesize, investigated, key="synthesize"))


async def main() -> None:
    dsn = os.environ["TASKQ_PG_DSN"]
    schema = os.environ["TASKQ_SCHEMA_NAME"]
    await taskq.migrate.apply_pending_locked(dsn, schema=schema, phase="pre")
    await taskq.migrate.apply_pending_locked(dsn, schema=schema, phase="post")
    pool = await asyncpg.create_pool(dsn)
    outcome = await run(app.get("deep_research"), pool, schema)
    print(f"outcome={outcome.outcome} report={outcome.result}")


asyncio.run(main())
```

Verified output:

```
outcome=terminal report={'findings': [{'sources': 3, 'question': 'q1'}, {'sources': 3, 'question': 'q2'}, {'sources': 3, 'question': 'q3'}]}
```

**The spawn face is LANDED** (T31 is closed — the child-flows lane's cure):
`ctx.spawn_flow(name, input=…, result=ChildResult)` is the sub-agent spawn/
join across run identities — the child is a REAL flow (its own run rows; the
run explorer, the admin and the CLI all see it), joined through the hold
machinery into the typed outcome union `ChildOk[ChildResult] |
ChildFailed | ChildCancelled | Expired`. The retry ADOPTS the same child
(the spawn's run key is the two-claims law at the flow level — a re-fired
body re-derives the same slot, never a duplicate child), the depth bound
(default 32) bounds the recursion, and the child's holds are the CHILD's
own — the sub-agent's approval gate is addressable separately
(`HitlClient.list(run=child_id)`), resolved without touching the parent's.
The one-flow fan-out above stays the right shape for the FLAT deep-research
(the count known, the children are queue rows, the join is the map's);
`spawn_flow` is the face for the child that needs its own run identity —
its own holds, its own run key, its own cancellation.

## Pattern 4 — The progress push: the nodes report

The streaming faces compared: LangGraph's `stream_mode="messages"` hands you
token deltas; Trigger.dev streams chunks over a durable connection that
resumes from the last chunk; Temporal's Workflow Streams (preview, May 2026)
expose a run's live output. TaskQflow's face is coarser BY DESIGN and honest
about it: `ctx.progress(pct, message, data)` is best-effort — an in-memory
latest-wins buffer flushed at a ~50 ms cadence, never awaited on the network,
never touching the finalize path. The law: *observability degrades FIRST,
never correctness*. A lost emission costs freshness; a blocked node costs
correctness.

The typed door: declare `progress_schema=` on the step and every emission
validates against the model — the declaration is what makes a SEPARATE UI
render the emission.

```python
import asyncio
import os

import asyncpg
from pydantic import BaseModel

import taskq.migrate
from taskq.workflows import (
    FlowRunner,
    ProgressBackfilled,
    ProgressListener,
    ProgressUpdated,
    Promise,
    StepContext,
    WorkflowApp,
    build,
    step,
)


class Inquiry(BaseModel):
    question: str = "the deploy"


class Draft(BaseModel):
    answer: str


class Reading(BaseModel):
    stage: str  # the node's DECLARED emission shape — the UI's contract


async def read_sources(ctx: StepContext, inquiry: Inquiry) -> str:
    for pct, stage in [(25, "searching"), (60, "reading"), (90, "synthesizing")]:
        await ctx.progress(pct, f"{inquiry.question}: {stage}", {"stage": stage})
        await asyncio.sleep(0.2)  # the work between reports
    return "the sources are read"


async def write_draft(ctx: StepContext, sources: str) -> Draft:
    return Draft(answer=f"the draft from: {sources}")


app = WorkflowApp()


@app.workflow("research_progress")
def research_progress() -> Promise[object]:
    sources = step(
        read_sources,
        Inquiry(),
        key="read_sources",
        progress_schema=Reading,  # a wrong-shaped emission is ProgressRefusedError
    )
    return build(step(write_draft, sources, key="write_draft"))


async def main() -> None:
    dsn = os.environ["TASKQ_PG_DSN"]
    schema = os.environ["TASKQ_SCHEMA_NAME"]
    await taskq.migrate.apply_pending_locked(dsn, schema=schema, phase="pre")
    await taskq.migrate.apply_pending_locked(dsn, schema=schema, phase="post")
    pool = await asyncpg.create_pool(dsn)

    runner = FlowRunner(app.get("research_progress"), pool, schema)
    flow_id = (await runner.create_flow(input=Inquiry())).flow_id

    async def watch() -> list[str]:
        # THE KNOCKS ARE POINTERS: a live ProgressUpdated carries the
        # seq + the source, never the payload — the ROW is the truth.
        seen: list[str] = []
        async with ProgressListener(pool, schema, flow_id=flow_id) as listener:
            async for event in listener.updates():
                match event:
                    case ProgressBackfilled():
                        seen.append("backfill")
                    case ProgressUpdated(source=src):
                        seen.append(src)
                if seen.count("notify") >= 3:
                    break
        return seen

    watcher = asyncio.create_task(asyncio.wait_for(watch(), timeout=30.0))
    await asyncio.sleep(0.5)  # the listener's backfill lands first
    await runner.drive(flow_id, until="terminal")
    knocks = await watcher
    # THE STATE CHANNEL: the node's latest-wins row — the payload the
    # knocks pointed at, plus the coalescing's own counter.
    row = await pool.fetchrow(
        f'SELECT pct, message, occurrences FROM "{schema}".wf_node_progress '
        f'WHERE channel = \'progress\' AND node_id = (SELECT id FROM "{schema}".jobs '
        "WHERE step_key = 'read_sources' "
        "AND (metadata->>'flow_id')::uuid = $1)",
        flow_id,
    )
    print(
        f"knocks={knocks} final_pct={row['pct']} "
        f"final_message={row['message']!r} emissions_coalesced={row['occurrences']}"
    )


asyncio.run(main())
```

Verified output:

```
knocks=['backfill', 'notify', 'notify', 'notify'] final_pct=90 final_message='the deploy: synthesizing' emissions_coalesced=3
```

The two channels behind the listener: the STATE channel (`wf_node_progress`,
latest-wins, constant under any rate — "where is this node NOW") and the
STREAM channel (`wf_node_stream`, the per-node 64-row ring with the dropped
count ON THE RECORD — "what did it report along the way", replayable with the
named `resync` frame when the ring pruned past a reconnecting cursor). The
aggregation is derived at read (a map's progress is its children's grouped
read), and the progress-lie fence holds: a body that reports 99% and then
fails shows **failed with pct=99 inside**, never 99%-running.

## Pattern 5 — The durable wait: the deadline is on the row

The frameworks' timers: Temporal's `workflow.sleep()` writes the timer into
event history (the worker can die for a month); Inngest's `step.sleep()` and
`waitForEvent` pause the function without holding infrastructure; Trigger.dev
`wait.for()` and waitpoint timeouts. TaskQflow's durable timer on the landed
surface is the HOLD's deadline: `timeout_s=` arms the DB-clock `expires_at`
on the signal row, the expiry sweep (the worker leader's arm in production —
the ONLY live timer on a held row) abandons the expired hold, the node
re-pends, the body re-runs from the top, and the wait site RETURNS the
union's `Expired` member — a typed VALUE the body matches, never an
exception it must catch.

The same law that makes this durable makes the slot cheap: **no sleeping
worker** — a held node is pending with a deadline, the worker released it,
the queue's capacity is back. A month-long approval costs zero occupancy.

```python
import asyncio
import os

import asyncpg
from pydantic import BaseModel

import taskq.migrate
from taskq.workflows import (
    Expired,
    FlowRunner,
    Promise,
    StepContext,
    WorkflowApp,
    build,
    step,
)
from taskq.workflows.api._hitl import sweep_expired_signals


class Check(BaseModel):
    what: str


class WatchResult(BaseModel):
    how_it_ended: str


async def watch_ticket(ctx: StepContext, check: Check) -> WatchResult:
    outcome = await ctx.wait_signal(
        (Check,),
        timeout_s=1.0,  # the fence's deadline is 1s so the sweep's pass is visible
        reason="the agent will re-check the upstream ticket when it moves",
    )
    match outcome:
        case Check() as hit:
            return WatchResult(how_it_ended=f"the ticket moved: {hit.what}")
        case Expired():
            # THE FAIL-CLOSE: the deadline passed on the DB clock — the
            # agent finishes with what it has instead of hanging forever.
            return WatchResult(how_it_ended=f"nothing moved on: {check.what}")


app = WorkflowApp()


@app.workflow("ticket_watch")
def ticket_watch() -> Promise[object]:
    return build(step(watch_ticket, Check(what="INC-4099"), key="watch"))


async def main() -> None:
    dsn = os.environ["TASKQ_PG_DSN"]
    schema = os.environ["TASKQ_SCHEMA_NAME"]
    await taskq.migrate.apply_pending_locked(dsn, schema=schema, phase="pre")
    await taskq.migrate.apply_pending_locked(dsn, schema=schema, phase="post")
    pool = await asyncpg.create_pool(dsn)

    runner = FlowRunner(app.get("ticket_watch"), pool, schema)
    flow_id = (await runner.create_flow(input=Check(what="INC-4099"))).flow_id
    held = await runner.drive(flow_id, until="held")
    await asyncio.sleep(1.5)  # the deadline passes on the DB clock
    abandoned = await sweep_expired_signals(pool, schema=schema)
    # (in production this arm is the WORKER LEADER's sweep — the dev-loop
    # drive does not run it; the fence calls it to make the pass visible)
    await runner.drive(flow_id, until="terminal")
    final = await runner.result(flow_id)
    print(f"drive={held} abandoned={abandoned} final={final}")


asyncio.run(main())
```

Verified output:

```
drive=held abandoned=1 final={'how_it_ended': 'nothing moved on: INC-4099'}
```

The expiry is AT-LEAST precision (it fires at the first sweep pass at-or-after
the deadline — never promise a precise wall time; the fail-close is the
guarantee). The deliberate re-wait is a NEW body decision (a NEW hold, a NEW
epoch — hold → expire → re-hold → ∞ is the convicted dragon the face refuses).

**The general timer is LANDED** (T30 is closed — the timers lane's cure):
`ctx.wait_until(when, key=…)` is the plain durable timer — the clock is the
hold's RESOLVER, no human and no payload model in the way. The wait is a ROW
(the same held representation: pending + a deadline, the worker's slot
released — the no-sleeping-worker law holds for timers), the row carries the
reserved `__timer__` prefix (invisible to `HitlClient.list` — a timer is not
a human's decision; a human resolve is refused), and the due-drain fires it
at-or-after the deadline (the wake returns the receipt: the key, the
scheduled instant, `fired_by='timer'`). A relative float or an absolute
datetime both work; the deadline is the DB clock's (never re-based). The
"re-check tomorrow" shape:

```python no-exec — not executed: the shape excerpt of a larger listing (the executed timer fences: this section's watch fence, the workflows guide's §4b, the docshelf guide's nightly cursor)
async def remind_body(ctx: StepContext) -> str:
    # `when = datetime.now(timezone.utc) + timedelta(days=1)` in production;
    # the fence's float is 1s so the verified run finishes fast.
    wake = await ctx.wait_until(1.0, key="remind")
    return f"wake key={wake.key} fired_by={wake.fired_by}"
```

The hold-deadline face above stays the right shape for the wait-for-SIGNAL
(a human or another flow answers); `wait_until` is the timer-only face —
the same hold machinery underneath, the clock as the only resolver.

---

## Where TaskQflow's shape is better

1. **The rows the operator can query.** LangGraph's checkpoint is a serialized
   blob keyed by thread; the OpenAI SDK hands you `RunState` and leaves the
   store to your app. TaskQflow's every face is a row an operator can SQL:
   the hold (listable, resolvable, auditable), the node (status, attempt,
   claim epoch), the ledger (`wf_step_ledger` — the exactly-once witness),
   the progress (state + stream). The diagnosis runbook is `taskq flows`
   against live rows, not a replay of a pickle.
2. **The no-sleeping-worker law.** A held node is pending-with-deadline and
   the slot is RETURNED — the queue's capacity is back while the run waits on
   a human for a month. The frameworks that block a thread or a process on
   the wait (the naive agent loop; OpenAI's in-process approval wait) pay
   occupancy for the pause.
3. **The run-key arbiter.** The concurrent dedup is the CLAIM's typed verdict
   (`created` / `existing-running` / `existing-terminal` — a failed run's key
   replayed is a LOUD refusal, never a silent 202). None of the seven
   frameworks has this layer: Inngest has function-level idempotency keys,
   Trigger.dev idempotency keys on waits — the arbiter's run-identity
   semantics (the second caller gets the EXISTING run's id and status) is
   ours.
4. **The typed expiry-as-value.** `Decision | Expired` with the checker
   forcing the fail-close arm: a body that ignores "nobody was watching" does
   not compile. The frameworks' timeouts are exceptions (Trigger.dev's
   `!result.ok`), silent resumptions, or hangs.
5. **The rate-limit terms stamped on the row.** `rate_limits=` rides the node
   declaration, the fork stamps the names onto the child rows — a run's
   admission terms are IN THE DATA, inspectable and re-readable by any
   consumer, not in the caller's config.

## Where TaskQflow's shape is worse

1. **The token-stream granularity.** `ctx.progress` coalesces to ~20 writes/s
   latest-wins; the frameworks stream token deltas and resume byte-exact.
   For a cowork-agent UI that renders the agent's answer AS IT THINKS, the
   coalesced face shows the shape of the work, not the words. (T32.)
2. **The checkpoint-the-whole-state simplicity.** LangGraph never asks the
   author what survives — every superstep checkpoints the whole state, and
   time travel (`get_state(history)`, replay/fork from any step) is free.
   TaskQflow's durability is explicit steps and edges; there is no declared
   whole-state snapshot and no fork-from-history API. The explicit face buys
   the operator's queryability and pays in authoring discipline. (T34.)
3. **The HITL ecosystem.** The hold row is the better STORE, but the
   frameworks ship the surrounding product: LangGraph Studio's interrupt
   visualization, the Slack-in-a-box approval layers built on the SDKs. Our
   resolve UI is yours to build (the SSE topic and the CLI are the
   primitives).

## Where we lack (the tickets, ranked by adoption impact)

Each ticket is design-first: the gap, the shape that closes it, the
adoption cost for the agent-builder who arrives from the framework that has it.

| Ticket | The named pattern | The one-line gap | The arrival cost |
| --- | --- | --- | --- |
| `tickets/T30-durable-sleep.md` | The durable sleep / wait-until | **LANDED — CLOSED**: `ctx.wait_until(when, key=…)` ships (the clock is the hold's resolver; the row is the truth; the map-index identity reaches the timer hold) — the guide's §Pattern 5 teaches it | — (was: an agent-builder from Temporal/Inngest reaches for the sleep on day one and finds nothing) |
| `tickets/T31-nested-flow-spawn.md` | The sub-agent spawn/join across runs | **LANDED — CLOSED**: `ctx.spawn_flow(name, input=…, result=G)` ships (the child is a REAL run — its own holds, its own run key, the retry adopts the same child; the typed outcome union) — the guide's §Pattern 3 teaches it | — (was: the deep-research story with per-sub-agent approvals stalls at the one-flow boundary) |
| `tickets/T32-token-stream-progress.md` | The token-stream progress class | Progress coalesces at ~20 writes/s; there is no fine-grained stream class for rendering the agent's answer as it thinks | The chat-shaped product shows the agent's shape, not its words |
| `tickets/T33-agent-memory-store.md` | The cross-run agent memory | The cross-run cache dedups; it does not REMEMBER — no namespaced store a run reads and writes for the run after it | Agents that "know the customer from last week" have nowhere to put it |
| `tickets/T34-time-travel-fork.md` | The time travel / fork-from-history | No declared API to replay/fork a run from an arbitrary node with new input | The "fix the prompt and re-run from step 3" debugging loop is a re-run from zero |

---

## The research receipts (the sources, the version dates)

Accessed 2026-10-12. Each claim in the mapping table traces to one of these.

- **LangGraph** — docs.langchain.com/oss/python/langgraph: *Interrupts*
  (`interrupt()` + `Command(resume=...)`, the checkpointer required, the
  thread_id as the persistent cursor); *Durability* / persistence
  (checkpointers per superstep; the durability modes; LangGraph 1.2's durable
  execution as the headline — the braiviq.com 2026 playbook and the
  vadim.blog durable-execution piece both date the durability framing to
  LangGraph 1.x, 2026). The known checkpoint-vs-side-effect seam (code before
  `interrupt()` re-executes; the forum thread on HITL payload persistence,
  2026) — the same re-execution doctrine we state as a feature.
- **OpenAI Agents SDK** — the `needs_approval` tool flag (bool or the dynamic
  callable), the serializable `RunState` + `state.approve(interruption)` /
  `Runner.resume`; the persistence/routing/audit left to the application
  (pliuz.com's comparison, contro1.com's integration docs; the xbstack.com
  RunState piece dated 2026-07-24, naming the 0.18.3 fixture). Sessions =
  conversation memory, not durable execution (datrick.com's production
  support review, last reviewed 2026-08-03).
- **Anthropic** — *Building Effective Agents* (anthropic.com/engineering):
  the five workflow patterns (prompt chaining, routing, parallelization,
  orchestrator-workers, evaluator-optimizer) and the workflows-vs-agents
  line; the patterns are durability-NAIVE by design — the durable-execution
  layer is the integrator's (Diagrid's "Checkpoints Are Not Durable
  Execution", 2026-02-25, makes exactly this point about LangGraph, CrewAI,
  and Google ADK).
- **LlamaIndex Workflows** — developers.llamaindex.ai: *Human in the loop*
  (`InputRequiredEvent` + `ctx.wait_for_event(..., waiter_id=...)`), the
  Context serialization (`ctx.to_dict(serializer=JsonSerializer())` +
  `Context.from_dict`) as the pause/resume story; the Workflows 2.x status
  and the checkpointing face vs LangGraph's (nutrient.io's 2026 comparison);
  the DBOS integration (the llama-agents-dbos package) as the third-party
  durability patch.
- **Inngest** — inngest.com/docs *Durable agent tool loops* (`step.run` per
  LLM call and per tool, the memoization contract, the token-usage ledger in
  the loop example); *AgentKit and step.ai* (blog, updated 2026-01-07); the
  flow-control faces (rate limits, per-tenant concurrency, throttle,
  idempotency); the Checkpointing dev preview (changelog 2025-12-10, the
  near-zero inter-step latency claim); the pattern table (sync sub-agent =
  `step.invoke()`, async = `step.sendEvent()`).
- **Temporal** — docs.temporal.io/ai: the AI Cookbook's human-in-the-loop
  recipe (Signals carry the approval; last updated 2026-08-14), the Approval
  and Entity Workflow design patterns, the deterministic-but-not-predetermined
  agent framing (temporal.io blog); the OpenAI Agents SDK integration GA
  (Mar 2026, per the Temporal-vs-Inngest comparison at wetheflywheel.com);
  Workflow Streams public preview May 2026 (same comparison).
- **Trigger.dev** — trigger.dev: v4 GA (the Run Engine, waitpoints),
  *Waitpoint tokens* (`wait.createToken({timeout})` + `wait.forToken` — the
  approval idiom), the v4.5.0 changelog (the durable chat: every turn a span,
  the stream resumes from the last chunk; `needsApproval` on the tool + the
  UI's `addToolApprovalResponse`; Sessions keyed on `externalId`).
- **CrewAI** — crewai 1.15.22 (2026-09-16, per Wikipedia's release table);
  the Flows' persistence/resume face (docs.crewai.com), the checkpointing
  pillar (replay from a step, fork, run with different inputs —
  crewai.com/open-source); the durability caveats (Diagrid's FAQ: CrewAI's
  native checkpointing is NOT the durable-execution layer; the hierarchical
  delegation defect, GitHub issue #4783, closed 2026 as not planned; the
  Atlan 2026 teardown).
- **The cross-cutting survey** — zylos.ai's *Durable Execution for AI Agent
  Runtimes* (2026-04-24): the convergence claim this page's mapping table
  embodies (persist completed boundaries; recover without repeating tool
  calls, approvals, or messages), and the Crab checkpoint/restore study
  (arXiv:2604.28138, 2026: >75% of agent turns produce no recovery-relevant
  state — the completeness argument FOR our explicit-steps shape and AGAINST
  blanket checkpointing, held honestly as evidence both ways).

*The fences' verification:* every ```python fence on this page ran under the
docs-example harness (`TASKQ_DOCS_EXAMPLES=1`, `tests/test_docs_examples.py` —
a fresh subprocess per fence, a private PG schema, the 45 s wall). The
captured outputs are pasted under each fence verbatim.

# Agent fleets — the LLM-gated research loop

An agent fleet is a service that runs autonomous loops — a research pass,
a tool call, a write — where SOME of the loop's continuations need a
human's yes. The loop has a budget (it may not run forever), the human
may not be watching (the loop may not hang), and the process may be
redeployed mid-run (the loop may not lose its state). Those three
"may not"s are the whole problem, and they are why the loop lives in
rows instead of in a process.

This guide walks one worked pattern end to end: the deep-research loop —
free passes, then an approval gate, then the report — using two shipped
example files:

- **`examples/deep_research.py`** — the loop itself: the gate, the
  budget walls, the fail-close.
- **`examples/deep_research_web.py`** — the approval board: one
  broadcast listener, per-user SSE streams, the resolve door.

The guide walks them line by line. The files run as written (their
verification lives in `tests/test_wf_demo_legs.py` and
`tests/test_wf_hitl_web_demo.py`, run twice for this guide's capture).

## The minimal gate, runnable

Before the full pattern, the smallest complete thing: a flow whose one
node runs, holds on a typed approval, and resumes on a human's answer.
This fence runs as-is under the docs-example harness (`TASKQ_PG_DSN`
supplied by the harness; both migration phases applied because the
example runs standalone):

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


class Research(BaseModel):
    topic: str


class ContinueApproval(BaseModel):
    approved: bool
    note: str = ""


async def research_body(ctx: StepContext, params: Research) -> str:
    outcome = await ctx.wait_signal(
        (ContinueApproval,), timeout_s=120.0, reason="continue past the free passes?"
    )
    match outcome:
        case ContinueApproval() as approval:
            return f"approved: {approval.approved} ({approval.note})"
        case Expired():
            return "nobody watching — finished with what we have"


app = WorkflowApp()


@app.workflow("mini_research")
def mini_research() -> Promise[object]:
    return build(step(research_body, Research(topic="notify-broadcasts"), key="research"))


async def main() -> None:
    dsn = os.environ["TASKQ_PG_DSN"]
    schema = os.environ["TASKQ_SCHEMA_NAME"]
    await taskq.migrate.apply_pending_locked(dsn, schema=schema, phase="pre")
    await taskq.migrate.apply_pending_locked(dsn, schema=schema, phase="post")
    pool = await asyncpg.create_pool(dsn)

    runner = FlowRunner(app.get("mini_research"), pool, schema)
    flow_id = (await runner.create_flow(input=Research(topic="notify-broadcasts"))).flow_id

    # Drive to the human gate: the run pauses as a ROW, the slot releases.
    await runner.drive(flow_id, until="held")
    print("held: a row, not a process")

    hitl = HitlClient(pool, schema=schema)
    holds = await hitl.list(run=flow_id)
    print(f"open holds: {[(h.node_key, h.reason) for h in holds]}")

    # THE HUMAN'S YES: one typed resolve by id (a second resolve of the
    # same hold is the defined no-op).
    delivered = await hitl.resolve(holds[0].hold_id, {"approved": True, "note": "ship it"})
    print(f"deliver: {delivered.status}")

    # The resume: the body replays from the top, the ledger memo makes
    # the replay cheap, the wait returns the DELIVERED answer.
    await runner.drive(flow_id, until="terminal")
    print(f"result: {await runner.result(flow_id)}")


asyncio.run(main())
```

Verified output (this guide's capture, on a fresh schema):

```
held: a row, not a process
open holds: [('research', 'continue past the free passes?')]
deliver: delivered
result: approved: True (ship it)
```

Three things to notice, because the whole pattern hangs on them:

1. **The hold is a row.** `drive(until="held")` returns while the run
   waits — no process, thread, or task is parked anywhere. A deploy that
   kills this process mid-hold loses nothing.
2. **The slot releases.** The held node's `locked_by_worker` is NULL —
   the worker that drove the run into the hold went on to other work.
   Concurrency is capacity, not waiters.
3. **The resolve is typed and idempotent.** The decision is validated
   against the gate's declared models BY SHAPE; delivering twice is a
   defined `no-op`, never a double resume.

## The worked example: the deep-research loop

`examples/deep_research.py` is the full pattern — this section walks it.

### The gate's payload and the carry

```python no-exec — not executed: verbatim fragment of examples/deep_research.py (verified: tests/test_wf_demo_legs.py drives this file; the capture rides the docs lane's report)
class ContinueApproval(BaseModel):
    """THE GATE'S PAYLOAD: the human's answer to "keep researching?"."""

    approved: bool
    note: str = ""


class ResearchState(BaseModel):
    """The carry: the notes so far + the gate's verdict + the report's
    named status (the fail-close's TYPED face)."""

    topic: str = "notify-broadcasts"
    notes: list[str] = Field(default_factory=list)
    approved: bool = False
    status: str = "researching"
    note: str = ""
```

The state is a pydantic model — the **carry**. The loop hands it to the
body each iteration and the body returns the next one; it is frozen at
spawn and advanced exactly once per iteration. A note lost on one hop is
unrepresentable: the carry is typed, so a field that isn't produced
can't be consumed.

### The iteration: free passes, the gate, the fail-close

```python no-exec — not executed: verbatim fragment of examples/deep_research.py (verified: tests/test_wf_demo_legs.py)
async def research_iteration(
    ctx: StepContext, carry: ResearchState
) -> Done[ResearchState] | Refine[ResearchState]:
    # THE FREE MARCH: the scenario's first three passes need nobody.
    if not carry.notes:
        carry = carry.research_passes(FREE_PASSES)
    if not carry.approved:
        outcome = await ctx.wait_signal(
            (ContinueApproval,),
            timeout_s=APPROVAL_TIMEOUT_S,  # the REAL default: 120.0
            reason="the research loop wants to continue past the free passes",
            tool="continue_approval",
            args={"topic": carry.topic, "passes_done": len(carry.notes)},
        )
        match outcome:
            case ContinueApproval() as approval:
                if not approval.approved:
                    # THE HUMAN SAID STOP: the same named result — the
                    # gate's note rides the state (the operator sees WHY
                    # it stopped).
                    return Done(carry.finish_with_what_you_have(note=approval.note))
                carry = carry.model_copy(update={"approved": True})
            case Expired():
                # THE FAIL-CLOSE: nobody watching — the loop finishes
                # with what it has (the typed expiry is a RESULT, never
                # a hang and never a crash).
                return Done(carry.finish_with_what_you_have())
    # THE APPROVED EXTENSION: one more pass per approved iteration; the
    # report ships at the bar.
    carry = carry.research_passes(1)
    if len(carry.notes) >= REPORT_PASSES:
        return Done(carry.model_copy(update={"status": _REPORT_COMPLETE}))
    return Refine(carry)
```

Read this as three decisions, each typed:

- **`Done(...)` / `Refine(...)`** — the loop's control union. `Done`
  ends the loop with a payload; `Refine` runs another iteration with the
  returned carry. There is no `while` loop you can forget to exit: the
  budget walls below bound it from outside too.
- **The wait's outcome is a closed union: `ContinueApproval | Expired`.**
  The `match` has an arm for each. A body that forgets the `Expired` arm
  is a checker error — the fail-close is FORCED, not hoped for.
- **Every exit is a named result.** The human said stop →
  `finish_with_what_you_have(note=...)`. Nobody watched →
  `finish_with_what_you_have()`. The bar was met →
  `status="report_complete"`. The run SUCCEEDS in every case, carrying
  the state that says which outcome happened. "The user not watching"
  is a value in the report, not an exception in the logs.

### The loop declaration and the budget walls

```python no-exec — not executed: verbatim fragment of examples/deep_research.py (verified: tests/test_wf_demo_legs.py)
CONTINUE_GATE = GateDecl(
    name="ContinueApproval", payload_models=(ContinueApproval,), timeout_s=APPROVAL_TIMEOUT_S
)


@dr_app.workflow("deep_research")
def deep_research() -> Promise[object]:
    """The loop IS the terminal: the Done payload (the ResearchState)
    is the run's result — readable from the rows alone."""
    research = loop(
        "research",
        research_iteration,
        initial=ResearchState(),
        max_iterations=6,
        budget_s=3600.0,
        on_exhausted="escalate",
        gates=(CONTINUE_GATE,),
    )
    return build(research)
```

The three walls, and what each is for:

| Wall | Value here | What stops |
|---|---|---|
| `max_iterations=6` | iteration count | a body whose `Refine` never terminates |
| `budget_s=3600.0` | wall-clock total, evaluated on the DATABASE clock | slow passes, accumulated waits; a held loop PAUSES the budget (`budget_paused`) — a human's thinking time is not the loop's spend |
| `timeout_s=120.0` | per-hold deadline | one hold outliving its window — expiry is the typed `Expired` value above |

`on_exhausted="escalate"` names what happens when a wall is hit: the
run enqueues to a REGISTERED escalation step (`loop.escalation`) — the
escalation lands, it is never a ghost actor. `gates=(CONTINUE_GATE,)`
declares the hold at COMPILE time: the hold is a visible node on the
graph, and the admin's Resolve form knows the payload's shape before
any run exists.

One shape law, honored on purpose: **a loop body declares ONE wait per
iteration.** The answer queue's cursor IS the iteration counter — a wait
on only SOME iterations would mis-index the delivered answers. The
scenario's "three free passes" are three passes INSIDE the first
iteration; the loop's own iterations are the approved extensions.

### The fail-close, stated as a contract

The 120-second window is the product's real default. If nobody resolves
in time, the body MATCHES `Expired` and finishes with what it has. This
is the deliberate design: nobody-watching is an expected event (lunch),
not an incident. A body that WANTS the failure — an approval that must
be explicit, expiry means stop-and-alarm — raises `SignalTimeoutError`
itself off the `Expired` member. The mechanism is the same either way;
the policy is the body's own `match` arm.

## The approval board: one listener, per-user streams

`examples/deep_research_web.py` is the web half — the hosting
application's own approval UI. Its five load-bearing decisions:

```python no-exec — not executed: verbatim fragment of examples/deep_research_web.py (verified: tests/test_wf_hitl_web_demo.py builds this app over its own pool)
@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncGenerator[None, None]:
    live_pool = await asyncpg.create_pool(pool) if owned else pool
    listener = HitlListener(live_pool, schema)
    await listener.start()
    _app.state.listener = listener
    try:
        yield
    finally:
        await listener.stop()  # every subscriber gets its own sentinel
        if owned and isinstance(live_pool, asyncpg.Pool):
            await live_pool.close()
```

**One `HitlListener` per process**, started in the app's lifespan: a
single dedicated LISTEN connection is the fan-out's hub. Every watching
browser is a subscriber to this one listener — do NOT open a LISTEN
connection per user (the capacity tax lands on the pool).

```python no-exec — not executed: verbatim fragment of examples/deep_research_web.py (verified: tests/test_wf_hitl_web_demo.py)
@app.get("/runs/{run_id}/events")
async def run_events(run_id: str, request: Request) -> StreamingResponse:
    """THE PER-USER SSE STREAM: the fan-out scoped by authz."""
    _gate(request, run_id)
    return StreamingResponse(
        stream_run_cards(_listener(), _client(), run_id),
        media_type="text/event-stream; charset=utf-8",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
```

**The stream is scoped by the app's own authz.** The demo's token→run
map stands in for a real identity system; the shape is the product's:
the forwarder only lets a run's own events through, so the data path
cannot leak another run's holds even if the endpoint wanted it to. The
admin's `/sse/holds` topic is the OPS surface (every hold, operator
session); THIS is the embedding face (one run, its user).

```python no-exec — not executed: verbatim fragment of examples/deep_research_web.py (verified: tests/test_wf_hitl_web_demo.py)
async def _render_frame(event: HoldEvent, client: HitlClient) -> str:
    if isinstance(event, HoldCreated):
        # THE ROW READ (leg 3): the pointer event fetches the context
        # the board displays (redacted exactly as the list/get surface
        # redacts).
        context = await client.get(event.hold_id)
```

**The event is a pointer; the row is the truth.** The broadcast frame
names the hold; the board reads the context (reason, deadline) from
`HitlClient.get`. The redact chain applies at the row read — a canary in
the tool args reaches neither the enumeration nor the knock. And the
union's fourth member exists for the outage window:

```python no-exec — not executed: verbatim fragment of examples/deep_research_web.py (verified: tests/test_wf_hitl_web_demo.py)
    if isinstance(event, Backfilled):
        # THE RECONCILE (the fourth member): forwarded verbatim — the
        # board drops any card the snapshot disowns.
        return f"event: backfilled\ndata: {event.model_dump_json()}\n\n"
```

**The `Backfilled` reconcile is taught verbatim** because it is the one
everyone skips: on subscribe — and after every reconnect — the board
drops any card the snapshot disowns. A hold resolved during an outage
never announces its own death; the snapshot is how the card disappears
anyway.

The resolve POST rides the same typed door the admin's form rides
(`HitlClient.resolve`), and the winning CAS's `HoldResolved` fan-out
clears the card on every subscribed stream — no polling, no refresh.

Runnable form of the whole demo:

```bash no-exec — not executed: the demo's run command (a long-running server, not a fence)
TASKQ_HITL_DEMO=1 TASKQ_PG_DSN=postgresql://... TASKQ_WF_SCHEMA=public \
  uv run uvicorn examples.deep_research_web:app --port 8088
```

## One run per slot: the per-run idempotency keys

A fleet's trigger endpoints get retried — the browser refreshes, the
queue redelivers, the cron double-fires. The run key is the answer:
**the same key twice is ONE run.**

```python no-exec — not executed: the trigger face (verified shape: examples/workflows.py's trigger_run + docs/guides/verify_guide.py's B3 verdict — same slot twice → identical flow ids)
runner = FlowRunner(dr_app.get("deep_research"), pool, schema)
claim = await runner.create_flow(input={"topic": topic}, run_key=f"research:{user_id}:{topic}")
# claim.kind: "created" | "existing-running" | "existing-terminal"
```

The claim is a TYPED verdict, not a bare id:

- `created` — this caller's new run → answer 202.
- `existing-running` — the concurrent duplicate's answer: the SAME run's
  id → answer 202 with that id (the arbiter dedups CONCURRENT triggers).
- `existing-terminal` — a TERMINAL run's key replayed: REFUSED-TO-REUSE,
  stated loudly, the prior run's id + status on the envelope. A failed
  run never silently re-fires under its own key — the re-run is the
  caller's documented choice of a NEW key (answer 409).

Per-run keys are also the cron story: a cron entry fires the slot key as
the run key — same slot twice (a tick overlap, a redelivery) → one run.

## The kill storm: what a deploy does to a mid-approval run

A rolling deploy kills worker processes while runs are held. The system's
behavior on kill is derived, not hoped:

- **The held run:** the hold is a row; the kill touches nothing it owns.
  On wake the body replays from the top — the research is deterministic
  over its corpus, so the replay is cheap — and the answer queue delivers
  the held epoch's answer in order. A resolved-during-the-outage hold is
  answered on the first wait of the replayed body.
- **The running node:** lease expiry re-claims the row for another
  worker. The reclaim never burns the ladder — infra fault is not body
  failure (resume-not-retry; the awaited ≠ failed ledger law).
- **The mid-fork kill:** the fork's children + edges + counter were one
  transaction; a kill at any statement boundary rolls it back whole, and
  the reclaim re-forks. A half-forked map is unrepresentable.
- **The run's status:** re-derived from rows ALONE — the step ledger's
  attempted terminals plus the node rows. There is no status cache to go
  stale and no orchestrator process to lose.

The operator faces the storm with one report: `taskq flows status
<run_id>` names where every run stands and, for a stuck one, the remedy
derived from the row's own reason.

## The operator's day

- **The run explorer** — the admin's `/taskq/workflows/{run_id}`: the
  graph view with the taken paths, the map's hexagon (its children
  addressed by `?map_index=N`), the held node's amber. The timeline
  reconstructs from rows alone; mid-hold it names the pending tool and
  gate.
- **The CLI verbs** — one question, one command: `taskq flows list`,
  `taskq flows status <run_id>`, `taskq flows holds <run_id>`,
  `taskq flows resolve <hold_id> '<json>' --app yourapp.workflows:app`.
  The write verbs validate against the bound gates' models and ride the
  audit rows; the read verbs are read-only, safe mid-incident.
- **The alert rows** — the shipped rules already page the fleet's real
  failure classes: `TaskQQueueDepthHigh` (the misrouted or starved
  queue's pending pile), `TaskQRunningLeaseExpired` (the kill-storm
  page), `TaskQFailedJobRateHigh` / `TaskQRetryRateHigh` (a body gone
  bad), `TaskQAbandonedJobs`. Each runbook section carries the
  confirm-SQL, the remediation, and the under-provisioned-vs-stalled
  distinction — see [Alert Runbooks](runbooks.md).

## Coming from a graph-checkpoint runtime

If your fleet today runs its agent loops on a graph runtime with an
in-process checkpoint store, the port is mechanical and the checkpoint
layer deletes entirely — the hold is a row, the resume is a resolve, the
durability is the database. The concept map and the worked port are in
[Migrating graph-checkpoint workflows](migrating-graph-checkpoints.md).

## Where the limits are (honest)

- **The agent loop inside a node is yours.** TaskQflow owns the durable
  DAG around the loop — the fan-out, the joins, the holds, the retries.
  Message accumulation and tool-call turns inside one node are the
  agent framework's job; the two compose by keeping the framework
  inside the step body.
- **Token-level streaming into a hold's UI** has no first-class engine
  channel. The two progress channels + the SSE faces are the shipped
  surfaces; a token stream is the embedding app's own stream.
- **The saga (compensation) surface is not built.** The ledger records
  every terminal outcome a compensator needs and the cancel cascade
  walks reverse edges — but there is no `on_failure="compensate"` verb
  yet. Hand-rolled compensation steps are the current answer; see
  [the pattern catalog](patterns.md).

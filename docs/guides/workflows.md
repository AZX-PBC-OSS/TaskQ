# Workflows — the DAG engine (`taskq[flows]`)

> **START HERE, newcomer:** the five-minute path is the next section —
> wire a DAG, run it, read the result, one fence. After it, this page is
> the engine's REFERENCE — it explains the machinery and pins every
> invariant. If you want the thirty-second tour of how to AUTHOR a flow
> (the verbs, the wiring, the hold), read **the API reference first**
> (`docs/api-reference/workflows.md`,
> §"the thirty-second tour"), then the worked example
> (`docs/examples/doc-ingest.md`), then come back here for the internals.

## The five-minute path

Everything you need to run a workflow: wire steps (the promise arguments
ARE the edges), fan items out with `map_source`, run ONE call, read the
decoded result. This fence runs as-is under the docs-example harness
(`TASKQ_PG_DSN` and `TASKQ_SCHEMA_NAME` supplied by the harness; set
`TASKQ_PG_DSN` yourself and the schema defaults to `taskq` — the same
default the settings carry):

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


class Batch(BaseModel):
    doc_ids: list[str] = []


class Enriched(BaseModel):
    doc_id: str
    text: str


class Report(BaseModel):
    published: list[str]


async def ingest_body(ctx: StepContext) -> list[str]:
    # THE RUN INPUT'S DOOR: `run(input=…)` seeds ctx.input ONLY — it does
    # NOT seed any body's wired params. The body re-hydrates the run's
    # input through the declared model's own validator (the tenant
    # pattern at THE BODY'S THREE INPUT CHANNELS below).
    batch = Batch.model_validate(ctx.input)
    return batch.doc_ids


async def enrich_item(ctx: StepContext, doc_id: str) -> Enriched:
    return Enriched(doc_id=doc_id, text=f"the text of {doc_id}")


async def publish_body(ctx: StepContext, enriched: list[Enriched]) -> Report:
    return Report(published=sorted(e.doc_id for e in enriched))


app = WorkflowApp()


@app.workflow("doc_pipeline")
def doc_pipeline() -> Promise[object]:
    ingested = step(ingest_body, key="ingest")  # no wired source: the body reads ctx.input
    enriched = map_source(ingested, enrich_item)  # the join: ingest.join
    return build(step(publish_body, enriched, key="publish"))


async def main() -> None:
    dsn = os.environ["TASKQ_PG_DSN"]
    schema = os.environ.get("TASKQ_SCHEMA_NAME", "taskq")
    await taskq.migrate.apply_pending_locked(dsn, schema=schema, phase="pre")
    await taskq.migrate.apply_pending_locked(dsn, schema=schema, phase="post")
    pool = await asyncpg.create_pool(dsn)

    the_corpus = ["doc-1", "doc-2", "doc-3", "doc-4"]
    outcome = await run(
        app.get("doc_pipeline"),
        pool,
        schema,
        input=Batch(doc_ids=the_corpus),
    )
    print(f"claim={outcome.claim.kind} outcome={outcome.outcome} result={outcome.result}")
    # THE INPUT FLOWED (the fence's own assertion): the published list is
    # the run input's list — a shape where `run(input=…)` does nothing
    # (a body reading a wired seed instead of ctx.input) cannot pass
    # here, because the output tracks the input exactly.
    report = Report.model_validate(outcome.result)
    assert report.published == the_corpus, (
        f"the run input never reached the body: {report.published!r} != {the_corpus!r}"
    )


asyncio.run(main())
```

Verified output (this guide's capture, on a fresh schema):

```
claim=created outcome=terminal result={'published': ['doc-1', 'doc-2', 'doc-3', 'doc-4']}
```

THE FENCE'S ONE LESSON, STATED WHERE THE FENCE IS (the full table is
[THE BODY'S THREE INPUT CHANNELS](#the-bodys-three-input-channels-one-table-one-place)
below): `run(flow, input=…)` seeds `ctx.input` only — the RUN's input,
the same value for every node of the run. It does NOT seed any body's
wired params: a body that declared a wired `Batch` param and ignored
`ctx.input` would run on the EMPTY seed forever (the wired param is the
node's OWN payload — a producer's decoded result, never the run input).
The fence's `ingest_body` reads `ctx.input` — the tenant pattern — and
its assertion makes the inert shape impossible: change the input, the
published list changes with it.

That is the whole loop: typed payloads in, a typed result out, every
step a row you can inspect. Where to go next, by the question you're
asking: the human gate → [Agent fleets](agent-fleet.md); the streaming
fan-out → [Event pipelines](event-pipeline.md); what you get vs the
other orchestrators → [the pattern catalog](patterns.md). The rest of
this page is the machinery those fences ride.

>
> **The reader's key** (the shorthand this page and the pin inventory
> use): `T07`/`T10`/`T18`/… are the work-stream's ticket numbers — each
> names ONE landed subsystem (T06 failed-parent propagation, T07 fan-in
> bounds, T09 the typed API, T10 holds/HITL, T18 retention coupling, T19
> loops, T20 the streaming router). `§N` references this page's own
> sections. `.measurements/…` citations point at the measurement
> artifacts' home on the evidence branch (`evidence/taskqflow`), not at
> files in this repo. "P3 rule N" is the engine's
> own rulebook (see the design docs under `docs/design/`; the layer's
> SPEC — the built truth, the kill list, the adoption pitch, the LATER
> rows — is [docs/design/workflows-spec.md](../design/workflows-spec.md)).

The workflow engine turns a fan-out/fan-in shape into queue rows with a
counter and a ledger: fork N children atomically, join when the ledger
says they are done, fire exactly once, deliver the declared consumers.
Nothing here is a second task system — every workflow row IS a `jobs` row,
dispatched by the vanilla claim path, and every invariant below is pinned
by a test that can fail (the pin inventory at the foot of this page).

```python
import taskq.workflows as wf
```

THE IMPORT LAW: `import taskq` never imports this package. The only
sanctioned entry is `import taskq.workflows` by a caller that opted into
the `taskq[flows]` extra. The engine is core-deps-only (tors, asyncpg,
uuid_utils); the extra exists as the operator-facing opt-in marker.

## The join-wait representation

A joined node waits as `status='pending'` + `deps_pending > 0` (+
`metadata.blocking_reason='join'`). No new ENUM value: `pending` is legal
in the vanilla machine, and the dispatch claim's `AND deps_pending = 0`
exclusion keeps join-wait rows unclaimable. When the counter hits 0 the
row becomes claimable by the claim predicate — no status write.

## The two-transaction finalize

`finalize_node` = **tx1** then **tx2**:

* **tx1** — the result write, carrying the TERMINAL-MARK FENCE
  (`status='running' AND locked_by_worker=… AND attempt=… AND
  claim_epoch=…`). A row the reclaim flipped, or a fenced attempt,
  updates nothing. The fork's child INSERTs + edge rows + join node share
  tx1 (a kill at any statement boundary rolls the whole tx back; the
  reclaim re-forks). The step ledger's terminal write rides tx1 (the
  ledger-terminal-atomic rule), and the failure IO-capture rides tx1. The
  ledger's write keys the claim's OWN RETURNING id (or the full arbiter
  tuple with `COALESCE(map_index, -1)`) — a map child's outcome can never
  overwrite its sibling's.
  **tx2 runs ONLY when tx1's fenced UPDATE returned a row** (the ROWCOUNT
  GATE): 50 duplicate finalizes → 1 decrement.
* **tx2** — the guarded decrement: one atomic
  `UPDATE jobs SET deps_pending = deps_pending - 1 WHERE deps_pending > 0`
  over the edge ledger, with the flow-status EXISTS leg inside the
  statement; rows hitting 0 → the guarded fire → only the guard winner
  runs the reducer body (INSIDE tx2) and writes the outbox rows.

## The reducer body's boundary

A raising reducer rolls tx2 back — the decrement, the fire row and the
outbox rows all roll back — and the re-derivation re-fires: the body
RE-RUNS. **At-least-once body execution; exactly-once for DB-local
effects** (the ledger claim + the composite arbiter), the stated boundary.

The tx1→tx2 crash window keeps the same guarantee ACROSS PROCESSES: the
leader's maintenance sweep runs `sweep_join_rederive`, which re-derives
the counter from the edge ledger and fires the firable joins. The sweep
heals schema-wide — ANY leader heals ANY flow's join-wait rows. The fired
join's body resolves through one door, in a fixed order: the fire arm
consults the process-local reducer memo FIRST; on a miss, the flow
root's stamped workflow name (`metadata.workflow` — `insert_flow_run`
writes it at create) decides the record's face. A stamped name that
resolves to NO compiled graph in the healer's process (the definitions
not imported there — a deployment defect), and a root with no stamp at
all, are the LOUD face: the delivery CONTINUES, the row is stamped
`blocking_reason='body_unavailable'`, and a WARNING names it. A stamped
name that resolves answers NO body: **the registry never supplies a
reducer body on the fire path** — a join has no body of its own (the
identity packer assembles its output from the parents' decoded
results), and a step's body executes at the row's CLAIM (the wired
args), never at the fire. No shipped `finalize_node` caller passes
`reducers=`, so the memo is never warmed and the healer never runs a
reducer body inside its transaction: the fire delivers, and every body
resolves from the registered definition at the fired row's CLAIM — the
durable leg (the BODY-FROM-DEFINITION discipline). The memo, when a
caller warms it, answers only in the process whose finalize warmed it;
it is bounded — the phantom reaper drops a terminal flow's entry at the
reap (a terminal flow's joins can never fire). A body that does run
inside the fire's transaction rolls the pass back on a raise, and the
next tick re-fires — the same at-least-once shape the finalize's tx2
states.

## The fan-in bound + the two join shapes (T07)

**The bound: ≤ 1000 declared parents per join** (`MAX_FAN_IN_PER_JOIN`) —
enforced at validate (`wf.validate()` / the validators): a join declaring
more is REFUSED, the error naming the bound and the child-driven
alternative. The refusal's class is
**`FanInBoundExceededError`** (a `ValueError` subclass, exported from
`taskq.workflows`) — the class the operator meets: it is the fork's
DETERMINISTIC authoring failure (the run record carries
`error_class='FanInBoundExceededError'`, the retry ladder never burns —
re-running cannot shrink the corpus), and its message carries the
remedy: the child-driven escape below, or partition the map.

THE HONEST DERIVATION (never a line through points that
don't share one): the three measured points — 200 → 14.9 ms, 1000 →
23.8 ms, 5000 → 39.3 ms — are sub-linear-in-log but NOT one line: an
endpoint fit (~5.1 µs/edge + ~14 ms base) predicts 19.1 ms at 1000
against the measured 23.8 — **the 1000 point is the outlier, named as
such** (least-squares gives ≈4.7 µs + 16.3 ms and still misses the
endpoints). The landed implementation's own refit
(`.measurements/edge-scale-curve.json` — the scale pin re-runs it) reads
**~1.7 µs/edge + ~2.7 ms base** (the artifact's refit block: marginal
1.684, base 2.705, the refit at the 1000 bound 4.39 ms, the outlier
residual −1.05 ms — the recorded points are 200 → 3.04 ms, 1000 →
3.34 ms, 5000 → 11.1 ms; the base-dominated criterion genuinely holds:
base 2.7 ms vs the 1000-edge marginal term ~1.7 ms), with the same
shape. THE GOVERNING BUDGET,
STATED HONESTLY: at the ~3-16 ms BASE term no fan-in meets a 5 ms-class
budget; the declared-edge cost is dominated by the BASE (paid once per
sweep pass regardless of fan-in), not the edges — the marginal edge cost
is µs-class. The bound stands on that BASE-COST argument — what it
bounds is the PER-JOIN marginal work inside one pass, not a per-edge
cost comparison.

**The child-driven escape** (`child_driven=True` on `map_source` /
`route` — the authoring verbs' own param; the engine-level spelling is
`JoinSpec(child_driven=True)`): past the
bound, the join does not trust the per-joined-row counter cache — the
fire counts its terminal children FROM THE EDGE LEDGER (each child's
finalize names its join target; the choice is RECORDED on the joined
node's `metadata.join_shape='child_driven'`). The exactly-once pins
(duplicate-finalize, double-fire PK) hold on BOTH shapes. OPERATIONAL
GUIDANCE: prefer partitioning the map over the escape — a fan-in past
32767 cannot even carry its count in the smallint counter cache (the
cache is a cache; the ledger is the truth — a >32767-unterminal recount
needs the counter's own widening before it can reconcile), so a genuinely
huge fan-in is a schema conversation, not a knob.

## The failed-parent propagation (T06)

A parent's TERMINAL failure — the retry ladder EXHAUSTED, or a
non-retryable class — resolves the joins counting it by the edges' DECLARED
failure policy, recorded on the edge ledger (`wf_edge.failure_policy`) at
fork/join time. The absorption is on the record; the derivation never
infers it.

**`fail_closed` (the default)** — the join fails CLOSED by a flow-scoped
transition set, ONE statement in the failing child's tx2:

* the joined node blocks: `metadata.blocking_reason='failed_parent'` +
  `metadata.failed_parent=<the failed node>` — its side of the counter is
  resolved by THIS stamp (the rederive arm locks `blocking_reason='join'`
  rows only), so the record never shows a hanging join;
* the running peers are PEER-CANCELLED: `status='cancelled'` with the
  cancel-origin marker `error_class='CancelledByPeerFailure'` (the `by`
  leg — the same outcome reads the same way whichever path produced it)
  and the structured record `metadata.peer_cancel =
  {"by": "peer_failure", "cascade_from": <the failed node>}`;
* the workflow fails: the flow root flips to `failed` (§17.2's cascade) —
  the linearization point every flow-status leg then reads.

The sweep composes: a child whose tx1 committed the terminal failure but
whose tx2 never ran (the crash window) is healed by the rederive arm's
`failed_required` count — the fail_closed join is blocked-with-reason by
the HEAL, never fired over the failed parent.

**THE TERMINAL-FAILURE TRIGGER:** the peer-cascade fires only on TERMINAL failure — a
child mid-ladder does not trigger it (a ladder retry emits no terminal, P3
decision 7), so a fail-closed map with a long-ladder child keeps its peers
running until exhaustion. BY DESIGN, not an oversight: over-rejecting
(murdering peers on an attempt-failure) is the worse asymmetry; the peers'
work is real work the flow may still need.

**`collect`** — child failures do NOT cascade. Each child runs to its own
terminal; at exhaustion the failure fans in as the typed `FailureInfo`
item, APPENDED to the join row's `metadata.failures` array, and the join
FIRES with the typed partial result. `FailureInfo` EMBEDS the estate's
error envelope — never re-spelled: `ErrorInfo(error_class, error_message,
error_traceback)` (`taskq.backend._protocol`, its bound constants carried
as-is), with `attempts` / `node_key` / `map_index` as the workflow-only
extensions. THE TYPE'S IMPORT FACE: `from taskq.workflows._types import
FailureInfo` (the wire-spec submodule — the documented path, the same
discipline the package root's surface law states; `ErrorInfo` is
`from taskq.backend._protocol import ErrorInfo`). `attempts` carries
the FULL attempt history (one entry per
ladder attempt, every error payload). The fan-in + the fire ride the
terminalizing tx: the fire is strictly AFTER the last ladder attempt
(the ts-ordered pin). A SKIP fans in with ZERO ledger rows — a skip is
not an attempt. The workflow SUCCEEDS with the failure report (the
absorbed-failure clause in the status derivation,
`docs/guides/observability.md`); the failure detail's home is the
ledger/attempts — the join row's array is the bounded summary (the
JSONB size-cap policy — see *The JSONB size cap / compaction policy*
below).

**THE CRASH CLASS IS THE COLLECT CONTRACT'S EDGE — know it before you
rely on the absorption.** The fan-in above runs on the child's FAILED
terminal (the ladder exhausted). A child killed MID-ATTEMPT (SIGKILL,
OOM, a lost worker) reaches the FAILED-CLASS terminal `crashed` through
the fleet's lease-reclaim sweep — and that crash-class terminal
currently contributes NO FailureInfo item to the join's
`metadata.failures`. What the join then shows, read off the rows: the
crash-class child occupies its slot in the packed list as `null` (its
slot is on the record — the decode of a typed sum REFUSES the null
loudly, `ValidationError … input_value=None`, and the run ends failed —
correctness is saved by the loud decode, never silently), with the
failures array EMPTY. The consumer that wants to survive crash-class
children today must type the join's union accordingly or decode
defensively; the crash record's home is the child's own row
(`status='crashed'`, its attempt rows) — `docs/guides/runbooks.md`'s
mid-run crash page has the operator path. (The engine cure — the
crash-class child's failure record riding the same collect fan-in — is
filed against `engine.py`'s failed-parent resolution, which currently
routes `outcome == "failed"` only.)

## The retention coupling (T18)

The pruner is WORKFLOW-AWARE: the result-expiry sweep may not eat a map's
children before the join fires (a child's `result_expires_at` is held
while a join-wait node counts it — after the join fires, the subtree ages
together, §10.3), and retention may not prune a parent row of a
NON-terminal run (the sweep's ledger recount reads parent rows as
truth). Terminal runs prune on the normal schedule. The guards are
WHERE-class conditions on the EXISTING arms — no new sweep process; the
failure mode the guards prevent is Oban-Pro's documented
`preserve_workflows` lesson (the reduce reading holes — the silent
partial). See `docs/guides/maintenance-sweeps.md` §7.

**The JSONB size cap / compaction policy (this note is the policy's one
home):**
the collect's FailureInfo list on the join row is BOUNDED — an append
that would exceed the byte cap (`FANIN_FAILURES_BYTE_CAP`, 64 KiB)
compacts the row to the bounded summary (the `"__truncated__": N` marker
counting the items no longer spelled + the ledger pointer); the FULL
detail stays on the ledger/attempts — the record never loses it. The
compacted row stays bounded forever (a later append increments the
marker).

## The three cancel legs

Cancel = one transaction — the flow flip is the linearization point; every
other statement re-checks flow status inside its own statement. THREE
legs, all shipped, any two insufficient:

1. the fire guard's flow-status leg (finalize tx2 and the sweep's fire);
2. the dispatch fence — the claim's candidate WHERE carries the
   flow-status EXISTS for workflow rows (a pending child of a cancelled
   flow is unclaimable; it refuses cleanly and the sweep re-derives);
3. the finalize fence (the terminal-mark CAS: worker + attempt +
   claim-epoch — the attempt is the fencing token).

## The step ledger + the run-key arbiter

The step ledger's claim is ONE round trip
(`INSERT … ON CONFLICT (flow_id, step_key, COALESCE(map_index, -1),
attempt) DO UPDATE … RETURNING`) — the ledger row IS the claim, the
arbiter physically blocks double-recording, and map children of one step
key are DIFFERENT claims. `ctx.substep("name", fn)` is the user-facing
shape: default idempotent ON, `idempotent=False` opts a step out.

**THE SUBSTEP HOLDS NO CONNECTION ACROSS THE BODY** (the LedgerWatch F4
cure — the measured tax, stated so nobody re-pays it): the claim and the
terminal write are ~1 ms statements, each on its OWN pool acquisition;
between them the body runs with NO connection held. The pre-cure shape
held ONE connection from the claim to the terminal — and the workflow
execution door runs bodies on the DISPATCHER pool (4 connections at the
shipped default), so a hot map fan-out's concurrent substeps collapsed
to the conn count: a 300-child fan-out measured the drop from 32 in-flight
to 4 (10.1 s → 27.1 s at the demo profile; 11.1 s → 42.2 s at 0.5
s/call). The exactly-once law
never needed the held connection — the claim arbiter linearizes the
grant, and the terminal keys the claim's OWN row id. Substep the calls
that cannot tolerate a re-run freely: the replay face is now
throughput-free.

Run-level idempotency claims against
the same composite arbiter with the scope `workflow-run:<flow name>` — a
conflict returns the EXISTING run's TYPED claim (`RunClaim`: `created` /
`existing-running` / `existing-terminal`), never a second silent
run, and two different flows sharing a naive key never collide (each
flow's keys namespace its own scope). THE RUN KEY'S SURFACE — two
spellings, one arbiter: `FlowRunner.create_flow(run_key=…)` (returns the
typed claim) and the PACKAGED one-call run,
`taskq.workflows.run(flow, pool, schema, input=…, key=…)` — create +
drive + the decoded result in one call, the typed claim on the returned
`WorkflowRunResult`. THE TERMINAL CASE IS STATED, NEVER SILENT: a
TERMINAL run's replay answers `existing-terminal` with the prior run's
id + status — the REFUSED-TO-REUSE verdict (a failed run squatting its
key silently was the founding bug class; the re-run is the caller's
documented choice of a NEW key). The demo endpoint surfaces the
distinction: 202 for created/existing-running, 409 for
existing-terminal. Cron composition: the cron entry
fires the slot key as the run key — same slot twice → ONE run.

THE CREATE IS ATOMIC: the root + the static nodes + the edges + the
root's start are ONE transaction — a kill at any statement window leaves
no ORPHAN ROOT behind (a run row with zero node rows is unrepresentable:
the maintenance derivation develops a run FROM its node rows, and a
nodeless root never derives, never terminalizes, never prunes — and it
would squat the run key forever). The belt for that conviction is the
`wf_nodeless_root_reap` sweep arm: any nodeless root that could ever
exist (a future statement-order regression's debris) is reaped `failed`
(`NodelessRunReaped`) once past the reap grace — the re-run is then a
NEW key, the typed claim says `existing-terminal`.

## The cross-run step cache (T25)

The arbiter's dedups are CONCURRENT and scoped: two callers racing one
run key get one run, and `ctx.substep`'s memo dedups a retry WITHIN a
run. Neither answers the temporal, cross-run question: the re-run (a
new run key, the same input) of an EXPENSIVE body — the OCR's pixels,
the LLM's tokens — executes everything again. The cross-run step cache
is the opt-in that closes it:

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
    run,
    step,
)


class Doc(BaseModel):
    doc_id: str


class Report(BaseModel):
    ref: str


calls = {"ocr": 0}


async def ocr(ctx: StepContext, d: Doc) -> Report:
    calls["ocr"] += 1
    return Report(ref=d.doc_id)


async def consume(ctx: StepContext, r: Report) -> str:
    return r.ref


app = WorkflowApp()


@app.workflow("cache_flow")
def cache_flow() -> Promise[object]:
    report = step(ocr, Doc(doc_id="d1"), key="ocr", cache=True, cache_ttl=3600.0)
    return build(step(consume, report, key="consume"))


async def main() -> None:
    dsn = os.environ["TASKQ_PG_DSN"]
    schema = os.environ.get("TASKQ_SCHEMA_NAME", "taskq")
    await taskq.migrate.apply_pending_locked(dsn, schema=schema, phase="pre")
    await taskq.migrate.apply_pending_locked(dsn, schema=schema, phase="post")
    pool = await asyncpg.create_pool(dsn)

    first = await run(app.get("cache_flow"), pool, schema, input=None, key="cache:demo-1")
    second = await run(app.get("cache_flow"), pool, schema, input=None, key="cache:demo-2")
    receipt = await pool.fetchval(
        f"SELECT metadata->'wf_cache_hit'->>'run_id' FROM \"{schema}\".jobs "
        "WHERE metadata->>'flow_id' = $1 AND step_key = 'ocr'",
        str(second.flow_id),
    )
    print(f"first={first.claim.kind} second={second.claim.kind} ocr_calls={calls['ocr']}")
    print(f"the hit's receipt names the producing run: {receipt == str(first.flow_id)}")


asyncio.run(main())
```

THE TWO-CLAIMS LAW — read it before opting in:

- **The arbiter dedups CONCURRENT** — two runs racing one key are one
  run. The cache does NOT dedup concurrency: two concurrent misses both
  run their bodies (that is the arbiter's claim, and the cache never
  takes it over).
- **The cache dedups TEMPORAL** — a LATER run over the same body and
  the same input reads the EARLIER run's result. The address is the
  Nix-style recursive hash: the body's code-version canon (the
  §22.1 stamper's — module, qualname, the body's own source) RECURSED
  with the resolved input's canonical jsonb. A body's code change IS a
  new address (the stale-code face is dead by construction); the same
  body over the same input at any position in any run is the SAME
  address (position-independent on purpose — the cross-run face's
  point).

The store is SUCCESS-ONLY: the result fills `wf_step_cache` on
terminal-succeeded only (`ON CONFLICT` keeps one winner per address; an
expired row is re-filled by the re-run's success). A failed run writes
NOTHING — the failed-run-squats-the-key bug is closed by construction;
the second run after a failure re-executes the body, never a cached
failure. A cache HIT delivers the cached envelope through the ordinary
fenced finalize: the body never runs, the downstream decodes the SAME
typed shape, and the node row's `metadata.wf_cache_hit` carries THE
RECEIPT — the address, the producing run's id, the store instant — the
hit is auditable from the row alone.

Verified output (this guide's capture, on a fresh schema — the SECOND
run's body never ran):

```
first=created second=created ocr_calls=1
the hit's receipt names the producing run: True
```

THE DETERMINISM HAZARD, stated plainly: the cache assumes the body is
DETERMINISTIC for the same input. A non-deterministic body — a
clock-reader, a random walker, a now() sampler — with `cache=True`
delivers the STALE TRUTH BY CHOICE: the address promises "same body,
same input", never "the world stood still". Opt in only for bodies
whose result is a pure function of the input; the default is OFF (the
cache is a decision, not a surprise), `cache_ttl` without `cache=True`
is refused at the wiring site, a non-positive TTL is refused, and a
cache on the multi-parent gather is refused (the v1 surface is the
plain step).

THE ADDRESS'S SCOPE — what "same body, same input" covers, exactly:

- **The code half covers the body's OWN source only** — module,
  qualname, the body's source text. **A changed HELPER is invisible to
  the address**: a helper's redeploy is a byte-identical address, and
  the PRE-DEPLOY truth serves until the TTL. That is the documented
  contract, and the operator's discipline is the cure: when the
  closure's truth changes, change the BODY's own source too (even a
  comment-line change moves the address). A transitive-closure hash
  (the helpers' sources hashed in) was considered and not built — the
  cost (a source-read walk over the whole call graph at the claim
  path, per attempt) and the false-positive face (an unrelated helper
  edit invalidating every address) are not justified by the hazard the
  discipline covers.
- **The input half covers the WIRED args only.** The FLOW input
  (`ctx.input`) NEVER enters the address. That is why `cache=True` on
  a body reading `ctx.input` is REFUSED at the wiring (E18): the
  cached answer produced under one flow input would serve a later run
  whose input differs — the cross-tenant leak (tenant A's answer
  delivered to tenant B, terminal-success, audits clean). For a SOURCE
  node the flow input is its only run-varying input, so a source body
  reading the run input has NO sound address either (the 640-row feed
  served its cached answer to the 60-row feed); the refusal teaches the fix.

The cure is the same shape for both faces — **lift the flow input into
the dataflow with an UNcached upstream step and wire its result as the
cached body's param** (the param enters the address, the value differs
per input, the cache caches per value):

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
    run,
    step,
)


class Tenant(BaseModel):
    tenant_id: str


class Report(BaseModel):
    ref: str


calls = {"priced": 0}


async def read_tenant(ctx: StepContext) -> Tenant:
    """The UNcached lift: the flow input's value enters the dataflow
    here — the flow input still never enters any address."""
    return Tenant(tenant_id=ctx.input["tenant_id"])


async def price_for(ctx: StepContext, tenant: Tenant) -> Report:
    """The CACHED body: the tenant is a PARAM — it enters the address,
    so each tenant's pricing caches under its own address."""
    calls["priced"] += 1
    return Report(ref=f"{tenant.tenant_id}:price")


app = WorkflowApp()


@app.workflow("tenant_cache_flow")
def tenant_cache_flow() -> Promise[object]:
    tenant = step(read_tenant, key="tenant")
    report = step(price_for, tenant, key="price", cache=True, cache_ttl=3600.0)
    return build(report)


async def main() -> None:
    dsn = os.environ["TASKQ_PG_DSN"]
    schema = os.environ.get("TASKQ_SCHEMA_NAME", "taskq")
    await taskq.migrate.apply_pending_locked(dsn, schema=schema, phase="pre")
    await taskq.migrate.apply_pending_locked(dsn, schema=schema, phase="post")
    pool = await asyncpg.create_pool(dsn)

    first = await run(
        app.get("tenant_cache_flow"), pool, schema, input={"tenant_id": "tenant-A"}, key="tenant:a"
    )
    second = await run(
        app.get("tenant_cache_flow"), pool, schema, input={"tenant_id": "tenant-B"}, key="tenant:b"
    )
    third = await run(
        app.get("tenant_cache_flow"), pool, schema, input={"tenant_id": "tenant-A"}, key="tenant:a2"
    )
    print(f"priced_calls={calls['priced']} (2: one per tenant — B's run paid, A's re-run hit)")
    print(f"A's answer: {await _ref(pool, schema, third.flow_id)} (never B's)")


async def _ref(pool: asyncpg.Pool, schema: str, flow_id: str) -> str:
    raw = await pool.fetchval(
        f"SELECT result->'value'->>'ref' FROM \"{schema}\".jobs "
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'price'",
        flow_id,
    )
    return str(raw)


asyncio.run(main())
```

Verified output (this guide's capture, on a fresh schema — the SECOND
run per tenant never re-ran the body):

```
priced_calls=2 (2: one per tenant — B's run paid, A's re-run hit)
A's answer: tenant-A:price (never B's)
```

Three more refusals close the remaining sound-address holes, each at
the wiring site (and re-proved by the validator's own rules — E17/E19/
E20):

- **A typed-route source is refused** (E17): `cache=True` on a source
  that carries `route(...)` arms wedges the run FOREVER at the first
  cache hit — the hit path cannot rebuild the route fork (the arm
  dispatch reads the element's runtime type; the cached envelope
  carries only encoded dicts), so the route's children and join never
  spawn while the source row reads `succeeded` wearing a cache receipt.
  Cache the route's arms' work, never the fork-spawning source.
- **An Exit-returning body is refused** (E19): the sentinel
  terminalizes through the exit finalize, which the success-only store
  never sees — `cache=True` would silently no-op (every run
  re-executes the body while the cache machinery looks armed).
- **An unverifiable body source is refused** (E20): a builtin, a
  partial, or an exec'd fragment cannot be hashed (the address
  requires the §22.1 canon — module + qualname + the body's own
  source), and the pre-refusal shape was the SILENTLY uncached step:
  the wiring accepted `cache=True`, every run re-executed the body,
  and the only trace was a log line. Define the body as a module-level
  def, or drop `cache=True`.

The TTL is the in-DB expiry (`expires_at`, compared against the DB
clock): an expired row IS a miss — the re-run re-executes and re-fills.
The sweep's retention arm (`wf_step_cache_retention`, the
`TASKQ_WORKFLOW_STEP_CACHE_SWEEP_PERIOD` setting, default 1h,
`timedelta(0)` disables) prunes the expired rows — the freshness
decision is the lookup's; the pruner only deletes the dead weight.

### Operating the cache: the eviction face

A well-shaped lie at a known address — a body that was deterministic
when it stored and is not anymore, a poisoned payload, a result produced
under a bug since fixed — serves every future run until the TTL with no
way to take it out. The operator's face is the `taskq wf` surface:

- `taskq wf cache list [--address-prefix PREFIX] [--limit N]` — the
  cache's rows (the address, fresh/EXPIRED, the producing run, the
  store and expiry instants) AND the hits' `wf_cache_hit` receipts (the
  hit runs' node rows, naming the address and the producing run each).
  The receipt already existed on the rows; this is its one surface.
- `taskq wf cache evict [--address-prefix P | --flow RUN_ID | --step
  STEP_KEY | --all] [--yes]` — the poison-pill door. **DRY-RUN by
  default**: the command prints the rows that WOULD go and touches
  nothing; `--yes` deletes. `--step` resolves the addresses through the
  step's own `wf_cache_hit` receipts (the only address↔step pairing
  the rows carry) — for an address no receipt names, use
  `--address-prefix`.

Eviction is immediate and one-way: the next run over an evicted address
is a MISS — the body re-executes and re-fills on its own success. The
lie's service ends when the operator says so, never silently.

### The fault classes at the finalize: one fault, one semantics

The operator's mental model for a transient blip (a connection storm's
`ConnectionError` class) landing on a cached step's finalize is the
RECLAIM class, symmetrically on both faces — the HIT's finalize and the
MISS's finalize share one machinery seat, and the classifier reads
WHERE the error escaped, never which face it came from. On either face
the infra-class fault writes the ledger row `crashed` (never
`failed`), the retry ladder is untouched, and the node row re-pends:
the run is reclaim-eligible, not dead. The cleared-fault re-drive
completes the step through the ordinary fenced finalize — and on the
hit face the body STILL never runs (that is the cache's whole point);
the `wf_cache_hit` receipt lands on the reclaimed row with the
re-drive as its producer. A cached step can therefore never
terminal-fail on a transport blip that struck after its body (by
definition) never executed.

## The sweep arms (wired)

Four healing arms register in the leader's maintenance sweep loop
(the `_SweepSpec` tick table), gated on the backend's workflow capability
marker (`workflow_sweeps_capable` — the same `hasattr` seam every
maintenance sweep is admitted through, each capability its own marker; a
backend that implements only the legacy maintenance surface keeps the
arms off):

* `wf_join_rederive` — the lock-first re-derive (`FOR UPDATE SKIP LOCKED`
  the join-wait children, count un-terminal parents from the edge ledger,
  reconcile the cache, stamp orphans, fire the firable) + the set-based
  fire;
* `wf_outbox_drain` — the delivery half: consumer rows inserted
  idempotently on the composite arbiter, the undelivered flag flipped in
  the insert's transaction;
* `wf_phantom_reap` — 'running' ledger rows on terminal flows fenced (the
  rows-alone reconstruction reconciles).
* `wf_nodeless_root_reap` — the orphan root's belt: a flow root with
  ZERO node rows (the create-atomicity's convicted debris shape) is
  reaped `failed` past the grace — the derivation can never develop it.
* `wf_step_cache_retention` — the cross-run step cache's expiry arm
  (T25): the expired rows pruned (the in-DB TTL's dead weight; the
  freshness decision is the lookup's — the pruner never touches a fresh
  row).
* `wf_edge_retention` / `wf_join_fire_retention` /
  `wf_step_ledger_retention` / `wf_signals_retention` /
  `wf_node_progress_retention` — THE FIVE FLOW-HISTORY ARMS (below).

The arms' imports stay inside the spec calls (nothing outside the package
may import `taskq.workflows` at module scope).

### The workflow tables' retention arms (the five periods)

Beside the two arms the sweep already owned — `wf_outbox_retention`
(`TASKQ_WORKFLOW_OUTBOX_RETENTION_PERIOD`, default 24 h) and
`wf_step_cache_retention` (`TASKQ_WORKFLOW_STEP_CACHE_SWEEP_PERIOD`,
default 1 h) — the leader's sweep carries FIVE retention arms for the
workflow tables' own rows, one setting per table:

| setting | the rows it retires | the rootless leg's row clock |
| --- | --- | --- |
| `TASKQ_WORKFLOW_EDGE_RETENTION_PERIOD` (`wf_edge_retention`) | the `wf_edge` ledger rows | the table has NO timestamp column — the rootless class deletes on sight (the pointee's absence IS the age proof) |
| `TASKQ_WORKFLOW_JOIN_FIRE_RETENTION_PERIOD` (`wf_join_fire_retention`) | the `wf_join_fire` rows (the exactly-once ledger's spent fires) | `fired_at` |
| `TASKQ_WORKFLOW_STEP_LEDGER_RETENTION_PERIOD` (`wf_step_ledger_retention`) | the `wf_step_ledger` rows (one per attempt — the family's fastest grower) | `created_at` |
| `TASKQ_WORKFLOW_SIGNALS_RETENTION_PERIOD` (`wf_signals_retention`) | the `wf_signals` rows (holds and their resolutions) | `created_at` |
| `TASKQ_WORKFLOW_NODE_PROGRESS_RETENTION_PERIOD` (`wf_node_progress_retention`) | the `wf_node_progress` rows | `updated_at` — a rootless row's flow is reached through the NODE's jobs row (`metadata.flow_id`; the table has no flow column) |

All five default to `timedelta(0)` = **OFF** — the deletion-sweep
family's zero-means-off sentinel (negative values refuse at settings
load) — but DELIBERATELY OPPOSITE to the outbox/step-cache precedents,
which default ON: those prune DERIVED data (a delivered outbox row's
narration, an expired cache corpse — deleting them loses nothing
re-derivable), while these five prune THE FLOW HISTORY ITSELF (the
attempt ledger, the fire ledger, the signals). Default-off keeps the
upgrade safe — no surprise deletes on an existing estate; the operator
opts in.

THE DEATH RULE is the FLOW's (a row dies with its flow, never by its
own age alone), and each arm runs TWO passes (the flows-side doomed
selection — the full-table-walk shape was convicted at production scale
and replaced):

* **THE FLOWS-SIDE PASS (every tick):** the TERMINAL-AGED ROOTS are
  selected FIRST from the JOBS side (`step_key='__flow__'` — the root
  identity, a terminal status, and the root's own `finished_at` past
  the period; served by `jobs_finished_at_idx`), then each selected
  flow's rows delete through the table's flow_id index, row-LIMITed to
  the batch. The flow read through its root is the same law the
  [retention coupling](#the-retention-coupling-t18) enforces one leg
  earlier (the guards keep the pruner out of a run's way while it
  lives): a live flow's rows are never touched, and a flow terminalized
  within the period waits it out. THE AGE CLOCK IS THE ROOT'S
  `finished_at` — the latest clock any of the flow's rows can carry
  (the root's age subsumes the row clocks); the comparisons run on
  `statement_timestamp()`, the prune family's selection-bound clock. A
  NULL-stamped terminal root (the stamp never landed) is SKIPPED —
  never doomed: no history is removed that has no honest age (a
  seconds-old flow's history must not vanish under a 30-day period
  because its stamp never landed) — and the arm's rate-limited
  `wf-retention-null-finished-at` warning fires when a pass OBSERVES
  one (an hourly re-warn cadence per table, never keyed to a
  deletion): fix the stamp or backfill it, the rows stay until they
  carry an honest age.
* **THE ROOTLESS PASS (a coarser cadence — every 8th call):** the
  orphan class — a row whose pointee is NOT a flow root at all (the
  root row absent, or a live NON-root job). It cannot be found from the
  flows side, so its statement is a PK-ordered walk that visits AT MOST
  the batch (the LIMIT bounds the SCAN) and probes each visited row's
  pointee once, deleting the row when the pointee is not a flow root
  and (where the table has a clock) the row itself is past the period.

Each pass deletes ONE bounded committed batch (`prune_batch_size` —
NOT a drain, the retention family's one-batch-per-pass discipline). And
the signals arm carries the ABANDONED-HOLD CLEANUP: a terminal flow's
HELD signals delete with the flow — nothing will ever deliver them (the
wait site is terminal) — while a LIVE flow's holds are never touched
(the deliver/resolve/expire arms own those).

## THE WORKER EXECUTION — the queue is the runtime, in production

A workflow row IS a `jobs` row, and the vanilla claim path dispatches it
— but a claimed row is work only if the claiming worker can RESOLVE the
body. Three cures ship, one machinery:

**1. THE EXECUTION FENCE (the claim's capability leg).** A worker that
never imported the flow definitions cannot resolve any body — handing it
a workflow row buys a claim-snooze-claim loop. So the claim statement's
fence admits flow rows ONLY when THIS round's worker registered itself
capable: the workers row's `metadata.workflow_execution` boolean, stamped
at boot. A non-capable worker NEVER claims a flow row — the row waits for
a capable worker (the defined pre-deployment state; the stranded-jobs
detector surfaces a flow nobody can run). The fence short-circuits on the
`step_key` probe — a vanilla row's plan cost is one column test, the same
cost class the P3 fence's own comment pins (the zero-tax law: the vanilla
claim path is unchanged; the dispatch band stays green).

**2. THE BOOT PROJECTION.** A worker whose
process imported at least one `WorkflowApp` boots CAPABLE: the projection
compiles every imported app's declared workflows (which is what populates
the definition registry in the worker's process — the body resolution
answers from these very compiles) and projects the compiled graphs'
`(actor, queue)` cohorts into `actor_config` rows — the SAME carrier, the
same `sync_actor_config` surface, the same drift guards the vanilla
`@actor` refs ride. The dispatch capacity LATERAL sees the workflow
cohorts; the admin's actors page, the drift machinery and
`TASKQ_QUEUES_STRICT` see them too — no second, invisible actor
population.

**3. THE INTERCEPT (the claimed row's execution door).** A claimed row
carrying the flow lineage (`metadata.flow_id`) executes through the
WORKFLOW machinery, never the actor registry: the body resolves from the
registered definition (the flow root's stamped `metadata.workflow`
name, the same durable leg the leader's fire arm uses) and runs via the
runner's ledger-claim + finalize machinery — the same two-transaction
finalize, the same fences, the same ladder. NOT a second execution
semantics: the in-process driver and the worker's door differ only in WHO
CLAIMED (the runner's own claim stamps `claim_epoch` 0; the fleet's
dispatch claim carries the row's epoch — every fence reads the epoch it
fences against). A row whose stamped name resolves to nothing (the
definitions not imported anywhere — a deployment defect, or a hand-crafted
row) parks at the snooze cadence budget-free, `released_reason=
'workflow-body-unresolvable'`, the stranded-jobs detector the witness —
LOUD, never a silent wedge.

### THE SPLIT PLACEMENT — one actor, one queue (the design law)

`actor_config` is keyed by actor name: ONE queue per actor, the estate's
own law. A flow's heterogeneous placement (§9.1 — a source on `default`,
a chain on `gpu`) is therefore expressed with **distinct actor names per
queue** — the chain's gpu step is a gpu-named actor:

```python no-exec — not executed: fragment, the author's bodies (screen_app, enrich_app, score_app) are the workflow's own
CHAIN = Chain(
    name="gpu-chain",
    start="screen",
    steps={...},
    actor="wf-gpu",
    queue="gpu",  # the chain's steps: a gpu-NAMED actor
)


@app.workflow("ingest")
def ingest() -> Promise[object]:
    src = chain_source(CHAIN, source_body, key="doc_source")  # actor 'wf', queue 'default'
    return build(src)
```

One actor name declared over TWO queues is a cohort conflict — refused
loudly (the boot refuses, the drift-guard precedent; the error names the
cure). The override-warning path (one queue silently winning) is dead by
construction.

### THE CAPACITY SURFACE — the declared admission terms

`@app.actor()` and `wf.step()` both accept the vanilla actor surface's
capacity fields — and, beside them, the vanilla enqueue's `priority=`
face (the rows' claim order) and the vanilla `start_to_close=` face
(the step deadline; the table's last row) — and the declaration
rides the SAME projection the placement rides:

| the term | what it declares |
| --- | --- |
| `max_concurrent` | the per-round admission **damper** — NOT a hard fleet-wide cap. The dispatch claim's per-actor arithmetic reads `actor_config.max_concurrent` and admits up to the value per round per producer, so the in-flight count can exceed it under concurrent dispatchers (the over-dispatch bound is `(num_producers - 1) * max_concurrent` per round — the vanilla field's own law, [Actors](actors.md#choosing-a-concurrency-control)). **The strict instrument is `reservations`** — declare one when the bound must hold. |
| `reservations` | the STRICT slots: a list of [`ConcurrencyReservation`](rate-limiting.md#concurrencyreservation) objects (`taskq.ratelimit.reservation`) — the dispatch-admission gate the vanilla path enforces via the leased `reservation_slots` rows (the machinery is [Rate limiting](rate-limiting.md)). The boot collects the nodes' declared instances into the worker's resolved registry and materializes their slot rows in the ensure-slots pass — the same pass the vanilla actors' reservations ride (deliberately NOT gated on the vanilla actor registry, so a pure-workflow worker slots its own). |
| `result_ttl` | the node result's in-DB TTL in seconds — the vanilla result-expiry sweep's own knob, seeded on the stored row. |
| `capacity_owner` | who owns the STORED capacity columns once the row exists (accepted on `@app.actor(...)` and on the route's `RouteArm(...)`; not a `step()` param). `"operator"` (the default) is the seed-only law: the literal seeds the row once, the operator owns it thereafter, and a later literal change LOGS a mismatch, never writes. `"code"` is the opt-out: the boot sync REWRITES `max_concurrent` and `result_ttl` from the literal on every boot, so a changed literal deploys by code instead of doing nothing after first boot (the queue and `max_pending` stay operator-owned under both owners). |
| `priority` | the rows' claim ORDER (the vanilla `enqueue(priority=…)` face, carried to the workflow surface): the value stamped into the `jobs.priority` column, which the dispatch claim's `priority DESC` orders by — the SAME claim SQL the vanilla rows obey, so a workflow step and a vanilla job compete on one vocabulary. THE PRECEDENCE LAW per row, first non-`None` wins: the **per-call** `run(..., priority=…)`/`create_flow(priority=…)` override > the step's declaration (`step(priority=)`, `RouteArm(priority=)` for its arm's children) > the actor's declaration (`@app.actor(priority=)`) > the column default `0`. The per-call face is the load-bearing one — an interactive run jumps a deep sweep backlog without touching its declaration (and the rows materialized AFTER create obey it too: the map/route children a worker forks, and the loop escalation's consumer row the outbox drain enqueues, all resolve the run's stamped override). The domain is the `smallint` column's (-32768..32767), refused at the declaration site like every other layer that accepts a priority. The map's children and the route's join land at the SOURCE step's resolution; the arms override DOWN from the source — DOWN is graph-DIRECTION, not a value ceiling: an arm's declaration governs its own children BELOW the source in the graph, and the value may raise or lower freely (an arm declaring `priority=9` over a source's 2 raises its children's rows to 9; the JOIN stays the source's 2). A handle body carrying `priority=` (`@app.actor(priority=5)` wired as the map body or an arm body) resolves as its children's actor level — the same precedence law, read at the fork. A spawned childflow run resolves its OWN declarations: the parent's per-call `priority=` does not cross the run boundary (the child run is a new `create_flow` — its rows read the child's own per-call/step/actor law; the parent's stamp governs the parent's rows only). |
| `start_to_close` | the step's DEADLINE (the vanilla `@actor(start_to_close=…)` face, carried to the workflow surface — a `datetime.timedelta`, enforced per ATTEMPT from the body's start): the value is stamped into the row's EXISTING `jobs.start_to_close` column at the node INSERT (no new column, no migration — the column the vanilla rows always carry), and the runner's body execution wraps in the SAME `_enforce_start_to_close` the vanilla consumer runs — the #791 TimeoutError-conflation cure (a body's own `TimeoutError` stays its own ordinary failure; only the deadline's expiry routes as the timeout), the tx-unwind budget, and the hostile-finally drills inherited. THE RESOLUTION LAW per row, first non-`None` wins: the **step's declaration** (`step(start_to_close=)`; a `RouteArm(start_to_close=)` for its arm's children — the arm overrides DOWN from the source, the JOIN staying the source's own cohort) > the **actor's declaration** (`@app.actor(start_to_close=)` — folded into the node at the wiring when the step declares none) > the **row's NULL** (nothing declared) > the **worker's `default_start_to_close`** (the enforcement-time fallback — the worker settings' value the fleet door threads; the dev-loop runner honors the row's stamp only). THE EXPIRY'S DISPOSITION: the attempt BURNS (a real execution — the ledger's failed row records the truthful `TimeoutError` / `start_to_close` pair, the vanilla rows' own stamp), the ladder re-pends at its own backoff curve until `max_attempts` then terminal-fails, and the run stays alive; a START_TO_CLOSE expiry is NEVER a Snooze — but a body raising `Snooze` WITHIN the window still defers (the deferral wins the race), and a body converting the deadline's cancellation into a `Snooze` LOSES (the clock's win is final — the vanilla enforcement's own race). THE CANCEL WINS over the deadline: a cancel mark on either carrier (the wf cancel's `metadata.cancel_phase` or the vanilla cancel's column mark) is fenced in-statement at the deadline's re-pend arm — the node terminalises `cancelled`, never re-queued under the deadline's backoff. The map's children land at the SOURCE step's resolution; the drain's outbox-materialized consumer rows inherit the downstream node's own declared bound through the fire's bindings. Not an actor_config axis (the deadline is the row's own column value — two steps of one actor may declare different bounds with no conflict law). |

`max_pending` is deliberately NOT on the workflow surface — it stays a
vanilla-actor parameter. The honest removal (never a dead knob): the
engine's fork/emit inserts bypass the enqueue admission path entirely
(mid-finalize, inside the engine's own transactions), and the claim side
cannot bound a backlog (claiming is the only drain — a claim-side gate
would wedge the backlog forever), so no enforcement point exists that
wouldn't structurally strand rows. Bound a workflow's backlog with
`max_concurrent` (the damper) or `reservations` (the strict cap).

THE TERMS FLOW: an `@app.actor(max_concurrent=1, ...)` handle wired via
`step()` carries its terms into the node as the node's ADMISSION
DEFAULTS — the explicit `step()` params override, never the reverse.
(PLACEMENT stays the step's own decision: the handle's NAME is not the
node's actor — the split-placement law above is unchanged.) The
non-negative-int law is the vanilla one, enforced at the declaration
site: a negative or non-int `max_concurrent` on
`@app.actor` is a `ValueError` before anything registers.

THE PROJECTION IS THE SEED-ONLY CAPACITY LAW — WITH ONE OPT-OUT: the
workflow actors' `actor_config` rows carry the declared caps — the SAME
law the vanilla `@actor` literals obey under the default
`capacity_owner="operator"`: the declaration seeds the row; the
operator owns it thereafter — override live via
`ActorsClient.set_capacity` (or `taskq actor-config set`), and **changing
the `@app.actor` literal later does NOT update a stored cap** (the
capacity fields are omitted from the config-sync's `SET` clause; a
literal-vs-stored mismatch logs, never writes). The opt-out is
`capacity_owner="code"`: the boot sync REWRITES `max_concurrent` and
`result_ttl` from the literal on every boot — a changed literal deploys
by code instead of silently doing nothing after first boot (the queue
and `max_pending` stay operator-owned under both owners). No new enforcement
machinery exists or is needed: a workflow row IS a `jobs` row, and the
dispatch claim's per-actor arithmetic reads `actor_config` for EVERY
actor, wf or not.

THE ONE-CAP-PER-ACTOR LAW: an actor name carrying TWO different capacity
declarations is REFUSED — `WorkflowActorCapacityConflictError` (the
queue-conflict law's twin on the capacity axes; raised where the boot
projection computes the terms, within one workflow and across
workflows/apps alike). `actor_config` is keyed by actor — one capacity
per actor, the estate's own law — so the capacity is the ACTOR's, not
the node's: give the differing node its own actor name (the error's
message names the cure).

### THE QUEUE-DEFAULT TRAP — declare the queue your workers run

Every wiring verb's queue default is `"default"` (`step`'s
`queue=`, the map's `map_queue=`, the `RouteArm`'s fallback, the chain's
rows, the loop's nodes). A deployment whose workers do not consume
`"default"` — a fleet of queue specialists — strands every such row
SILENTLY: the fork's INSERT succeeds, the dispatch claim matches only
the claiming worker's own queues, and nothing ever claims the row. THE
DISCIPLINE: declare `queue=` explicitly per step when your workers don't
run `default`. A queue named in no worker's consumed set FAILS LOUD,
gated on `TASKQ_QUEUES_STRICT` (the vanilla queues' own opt-in law):
strict — the boot REFUSES with
`WorkflowQueueUnconsumedError`; non-strict — the boot proceeds with the
ONE aggregated `workflow-cohorts-on-unconsumed-queues` warning (the
vanilla warning's own shape, the remedy named), because a per-queue
worker specialization legitimately imports the shared apps module while
a sibling worker consumes the foreign queues. The strictness also has a
SECOND, app-declared face — `WorkflowApp(queues_strict=True)`: the
contract declared in CODE, riding the app every worker child already
imports (the fleet whose children each subscribe to a queue subset
cannot coordinate a fleet-wide env var — it need not: the app's own
definition carries the contract). The faces OR-combine: the boot
refuses when EITHER is set, and the env can only ADD strictness — ops
tightening stays possible, silently disabling a declared contract is
not a thing. THE CROSS-APP TIGHTENING: in a process importing SEVERAL
apps, ONE app's `queues_strict=True` tightens the gate over the WHOLE
projected cohort set — a co-resident app's sibling-served cohorts
refuse too (the refusal names the queues and the cure). The check fires before
the cohorts' sync writes anything; both arms name the cures (add the
queue to a worker's consumed set, or route the step elsewhere). The
operator-facing entry is
[Upgrading](upgrading.md#workflow-steps-default-to-the-default-queue).

### The orchestration-only drive

`drive(flow_id, execute=False)` is the fleet-mode driver: the driving
process claims and executes NOTHING — the tick runs only the sweep arms,
and the flow's work executes in the fleet's workers through the
queue-routed dispatch. The `execute=True` default is the dev-loop driver,
unchanged. Each node on ITS pool: a `default`-subscribed worker executes
the source, a `gpu`-subscribed worker the chain — the queue column is
load-bearing end to end. THE DOORS DIVIDE MORE THAN PLACEMENT: the
worker's dispatch claim is also where the declared `rate_limits=` bind
(the in-process dev-loop door runs no rate-limit acquire — see the
rate-limiting guide's "which door honors the buckets"). Any behavior
that lives at the CLAIM (rate limiting, queue caps, fairness) is a
fleet-door behavior; the dev-loop door is for the wiring's logic.

## The build-time validators

A malformed graph is a coding error, refused before any row is written:
`validate_fork` refuses the empty fork that carries NO join (zero
children, nothing to collect — the stranded invisible join's shape);
an empty fork WITH a join is the legitimate shape (the born-zero
receipt: the route/map over a legitimately-empty
corpus declares its join, the join is BORN `deps_pending=0`, fires
immediately, and packs the EMPTY list — `result() == []`, the run
terminal, SUCCESS truthful: a night with no documents chunked zero
documents, and that is correct); `validate_join_spec` refuses a join node with zero incoming
edges (the stranded invisible join — before the sweep's LEFT-JOIN
hardening it was not even diagnosable). The runtime backstop: the rederive
arm enumerates edge-less join-wait rows (a LEFT JOIN on the edge ledger)
and stamps them `metadata.blocking_reason='orphan_parent'` — the record
never looks healthy while the work is wrong.

`wf.validate()`'s own rule matrix (T09, checker-independent): **E1** acyclicity · **E2** produced-never-consumed ·
**E3** the edge-less join (the compiled graph is public data — the rule
owns the shape injected into it, not just the verb's door) · **E4** the
unannotated step · **E5** the incompatible consumer — including the
DUCK-TYPED hole (an unannotated/`Any` param consumes a model producer
unseen) · **E6** the fan-in bound · **E7** the cross-graph promise (a
promise wired from ANOTHER app's recorder — recorded by the verbs,
convicted here; the colliding-key smuggle builds a silently wrong edge) ·
**E8** the loop CARRIER-TYPE (a body refining an unrelated model against
the declared `carry_type=` (or the model instance passed as `initial=`) — the recorded declaration is enforced) · **E9** the ctx
annotation (must BE `StepContext` or the declared-unchecked
`Any`/`object`) · **E10** the arity (the body's params beyond ctx must
match the wired sources' count) · **E12** the deps contract (below) ·
**W1**
the eternal wait (the warning class) · **W2** the unknown queue (a queue
no actor declares and TASKQ_QUEUES does not name — the warning class;
the worker-boot fail-fast stays the runtime door).

## THE DEPS SEAM — the body's dependency door (the DI capability)

### THE BODY'S THREE INPUT CHANNELS (one table, one place)

A body's parameters arrive through THREE distinct channels — the
convention is positional, and every wiring question ("why is my param
the run input?") is answered by which channel a param rides:

| the channel | how it arrives | what it holds | declared by |
| --- | --- | --- | --- |
| **the context** | the FIRST parameter, always `ctx: StepContext` | the RUN's machinery: `ctx.input` (the run's input — the SAME dict for every node of the run), `ctx.progress()`, `ctx.substep`, the emit batch face | fixed — never wired |
| **the wired payload** | the params AFTER `ctx`, in the wired order | the node's OWN data: each wired source's DECODED result (a producer's model, the map's item, the join's collected list), then the seed payload's plain args (the body call convention: `(ctx, wired_source_1, …, wired_source_n, plain_args…)`) | the wiring verbs' arguments (E10 owns the arity) |
| **the deps** | the LAST parameter (opt-in) | the app's bound `Deps` instance — clients, store handles; the SAME instance at every node | `deps: MyDeps` annotation + `WorkflowApp(deps=…)` (E12 owns the contract) |

The two dict-shaped channels are the common confusion: `ctx.input` is
the RUN's input (every node sees it — use it for run-wide facts: the
run key's dimension, the feed spec read once); a node's OWN payload is
its wired params (the producer's return, decoded). `run(flow, input=…)`
seeds `ctx.input` only — it does NOT seed any body's wired params and
does NOT participate in the cross-run cache's address (the address keys
on the WIRED payload, see the cache section).

Step bodies need dependencies — a client, a store handle, a queue —
WITHOUT reaching through `ctx` (the context is the RUN's contract, not a
service locator) and without module globals (untestable, unswappable).
The seam: declare a `Deps` dataclass PER APP and bind ONE instance at
the door; bodies OPT IN by declaring it:

```python no-exec — not executed: excerpt — the deps SEAM's declaration shape (the bodies' names live in the app's own module; the RUNNABLE seam demo is examples/workflows.py's EnrichClient, verified: tests/test_wf_demo_legs.py)
from dataclasses import dataclass

from taskq.workflows import Promise, StepContext, WorkflowApp, build, map_source, step


@dataclass(frozen=True, slots=True)
class EnrichDeps:
    marker: str = "enrich-client"


app = WorkflowApp(deps=EnrichDeps())  # ONE instance, bound ONCE at the door


@app.actor(queue="cpu")
async def enrich(ctx: StepContext, doc_id: str, deps: EnrichDeps) -> Summary:
    # the THIRD parameter is the deps contract — the runner hands the
    # app's bound instance in; the signature is the opt-in.
    return Summary(doc_id=doc_id, text=deps.fetch(doc_id))


@app.workflow("doc_ingest")
def doc_ingest() -> Promise[object]:
    ingested = step(ingest_body, IngestBatch(doc_ids=["d1"]), key="ingest")
    enriched = map_source(ingested, enrich)  # the map's children get the SAME instance
    return build(step(publish_body, enriched, key="publish"))
```

The contract, enforced at build by **E12-deps-contract** (the SAME arity
discipline E10 owns, extended honestly — one param beyond `ctx` + the
wired sources IS the deps shape):

* a body declaring the deps shape where the app binds **no** deps is
  refused at `app.get(...)` — the message names the fix (bind `deps=` on
  the app, or drop the parameter: it is never a fourth data source);
* a body whose deps annotation the bound instance does not **satisfy**
  is refused — both type names in the message;
* a true arity mismatch (two params beyond the wiring) stays **E10**'s.

The bound instance is THE instance — the step bodies, the map's item
children, the loop driver's bodies, and the hold-wake's re-execution all
receive the same object (bound once at the door, never re-minted per
claim). The runner never inspects it (`object` through the runner); the
TYPING is proven at the decoration — E12's runtime check of the body's
declared type against the binding. Every door binds the same way:
`WorkflowApp(deps=…)` (the compile carries the binding — the packaged
`run(flow, pool, schema)` and the worker-hosted execution door read it
from the compiled object), or `FlowRunner(…, deps=…)` / `run(…, deps=…)`
(the direct doors — the runner's binding wins, and the validation
re-runs against the effective binding). The InMemoryBackend has NO
workflow execution surface (the named landmine): the seam's test face is
the real `FlowRunner` against the pg fixtures.

## The pin inventory

Every invariant above is pinned by a test that can fail, red-first: each
pin's red is a REAL mutation of the shipped engine (a guard flipped, a
statement mutated, a resolver swapped), observed and recorded to
`.measurements/` — never a local re-implementation of the dragon. The
families:

| file | family |
| --- | --- |
| `tests/test_wf_schema_migration.py` | the schema round: the three-file lock-class split, the partial-index doctrine, the import law, the dispatch exclusion |
| `tests/test_wf_finalize_pins.py` | the finalize family: the deps fingerprint (pin 1), the rowcount gate (6), the attempt fence (13), the gremlin guards (23: the `>= 0` flip; 24: the flow-status leg) |
| `tests/test_wf_sweep_pins.py` | the sweep family: the post-cancel fire refusal (2+5), the misnamed child (8), held-row exclusivity (4), the phantom reaper (15), the empty join (17), the dispatch fence (2's claim leg), the sweep-arm wiring (21), the bounded reducer cache (22) |
| `tests/test_wf_fork_pins.py` | the fork family: the id-collision (18), fork atomicity (19), the outbox drain exactly-once (20) |
| `tests/test_workflows_ledger_pins.py` | the ledger family: the double-run (1), the lost-completion window (2), the concurrent claim (3), the run-key replay (4), the claim-atomic window (5), the terminal-atomic split-write (6), the map-children arbiter (7) |
| `tests/test_wf_engine_units.py` | the in-process pins: the seam-only generation (10), redact-before-persist (11), the canonical hash (12), the deadlock budget (14), body-from-definition (16) |
| `tests/test_wf_propagation_pins.py` | the T06 propagation family: the stranded join (pin 1), the peer-cancel record (pin 2), the collect exhaustion fan-in + the ts ordering (pin 3), the skip's zero ledger rows (pin 4), the mid-ladder composition (pin 5), the sweep's crash-window heal (pin 6), the policy validator (pin 7) |
| `tests/test_wf_loop_pins.py` | the T19 loop family: the two walls (the budget's paused-blind arm, the iteration cap's advance-shared guard), the carry's frozen-at-spawn single-apply (the typed carry at iteration 0, the resume's exact sequence), the advance/exhaust statements' claim fence (the zombie strike), the union return's named failure, the exhaustion's honest count, the waits-forever warning, the carrier-type's registration refusal, the held iteration's invisibility |
| `tests/test_wf_hitl_pins.py` | the T10 HITL family: the hold/resume contract, the deliver's CAS, the timed hold's expiry, the cancel cascade's hold leg |
| `tests/test_wf_phase3_cure_pins.py` | the phase-3 fix round's family: the typed early-exit sentinel, the manual resume, the map-join consumption door, the T10/T19 blockers' cures |
| `tests/test_wf_progress_emission.py` + `test_wf_progress_faces.py` + `test_wf_progress_persistence.py` | the T21 progress family: the emission op's typed gate (the validated dump is the record), the coalesce cadence + the constant rows, the auto projections' counted trims, the two-channel persistence, the faces' contracts |
| `tests/typeprobe/` | T01's negative type probes (pyright + ty, the CI `type-probes` gate) |
| `tests/test_wf_perf_bands.py` | the perf bands: the 1000-child fan-out tx, the join-fire latency, the enqueue/dispatch noise bands, the T07 edge-join scale curve (the refit + the 100k-edge plan assert) |
| `tests/test_wf_step_cache_pins.py` | the T25 cross-run cache family: the hit (the body runs ONCE across runs; the receipt on the node row), the success-only law (a failed body's key never squats), the TTL's expiry (the scaled clock: expired is a miss, the re-fill beats the corpse), the CAS (two concurrent misses, ONE winner), the typed decode at the hit, the address's code sensitivity (the stale-code face dead), the wiring refusals (the TTL without the opt-in, the non-positive TTL, the gather), the retention arm's fresh/expired split + the disable sentinel |
| `tests/test_wf_timer_pins.py` | the T28 durable-timer family: the at-least wake (never early, the receipt's arithmetic), the slot's release (the held representation, the ledger's `awaited`), the crash-mid-wait durability (the successor's sweep fires the wake), the cancel interplay (the hold dies with the flow), the Expired/timer orthogonality (both named outcomes fan in), the resolve boundary's refusal (the clock is the only resolver) |
| `tests/test_wf_versioning_pins.py` | the T29 versioning-contract family: the redeploy face (the adopt at the next node claim + the stamps proving WHICH code ran each row), the mid-wait redeploy (the wake re-enters the NEW body, the replay law across the deploy) |
| `tests/test_wf_otel_pins.py` | the wf-otel family: the run span (the root + the ambient link, the attribute contract, the lifecycle events, the terminal's status), the step spans (the children in-process, the fleet door's per-process root, the attribute contract), the ladder's scrubbed failure (the retried/terminal routes, the DSN canary), the deferral/hold/fork/join events, the crashed face, the no-op law (telemetry off, the provider absent), the rows-identical parity |

History note: T03's schema-pin file (`tests/test_workflows_schema_pins.py`)
was folded when T05 landed — its pins live in
`tests/test_wf_schema_migration.py` (the schema/lock-class/import families)
and `tests/test_wf_perf_bands.py` (the T03 bands); no pin was dropped.

| `tests/test_wf_t20_maintain_liveness.py` | T20's core fix: the failed arm gates on live UNRESOLVED work (the 149-siblings scenario red-first; the wedge-cure-stands regression) |
| `tests/test_wf_t20_emit_pins.py` | T20's emit tx: the kill-storm at every statement window (zero re-emitted, zero lost), the between-pages kill, the zombie fence, the map_index discipline |
| `tests/test_wf_t20_fence_probe.py` | T20's fences: the 30-sweep premature-terminal probe (no worker — nothing terminalizes), the mutation drill's teeth, the source-terminal subject |
| `tests/test_wf_t20_router_pins.py` | T20's router: both totality doors + the flips, the route through the certified fork, the end-to-end author surface, the loud refusal's row |
| `tests/test_wf_t20_backpressure_pins.py` | T20's backpressure law: the pager blocks at the declared bound (the unbounded variant's blow-out captured red), the throttle releases under a draining worker, the wide-page refusal, the workflow-level declare end to end (the stalls ride the ladder, the resume from the cursor) |
| `tests/test_wf_t20_crashed_terminal_wedge.py` | THE TERMINAL-CRASH SEMANTICS (the T20/T21 fixer's wedge cure): the corpse derives the failed-class terminal (red-first: it derived `running` forever), the wedged root heals on ONE maintenance pass, the red-drill (the assertion REDS on a corpse the heal cannot land — the no-op-maintenance mutant), the reclaim-input asymmetry (the ledger's attempt-level crash stays live), the zombie-audit structural pin (the liveness spellings) |
| `tests/test_wf_t2021_gate_cures.py` | T20/T21's gate cures: the VALIDATED dump is the record (the lax-pydantic string hole), the buffer door is private + the public op validates, the pct domain CHECK is real at the storage domain, the auto class's drop receipt, the failure-class ROUTING per class (3 deterministic legs + the transient leg), the poison record kills ITSELF loudly — never its mates |

## The ergonomic contract (T17 — the authoring session's paper cuts)

The API's bar is **"first-try correct, no boilerplate, IDE
autocompletion resolves the wiring."** The authoring session's paper
cuts are standing law; the COUNTS HAVE ONE HOME —
`.measurements/t17-dispositions.md`, the disposition ledger (its line
is derived from its own table; this guide carries no arithmetic of its
own). Each cure's CONTRACT, and where to read it:

| Paper cut (the stumble) | The cure's contract | Landed in |
| --- | --- | --- |
| "Nothing runs after the join" | A join's user reducer body is spelled **in the wiring** — the fan-in node IS the reducer; its consumers dispatch as normal steps | flow API (T09) |
| "No sequencing — a depth-3 DAG didn't fit one flow" | Sequencing is dataflow: consume a node's promise downstream and it dispatches strictly after; nested maps compile | flow API (T09) |
| "`deliver()` dropped my payload" | The signal row CARRIES the payload; the resumed body reads it | HITL (T10) |
| "A second hold on the same name exploded" | A new hold mints a NEW epoch — the identity is the hold epoch, not the name; the same signal can hold again | HITL (T10) |
| "The guard was decided at create time" | Skip guards are evaluated **at dispatch**, against the flow's state | flow API (T09) |
| "My holds ate the retry budget" | RESUME-NOT-RETRY: a hold's resume consumes NO ladder attempts — the ladder counts failures only | HITL (T10) |
| "The parent came from splitting a string" | The parent is a COLUMN; bad node-key spelling is refused at compile, never mis-derived (this engine — verified) | core (T03/T04) |
| "`create_flow` took no input" | The run carries its input; bodies read `ctx.input` | flow API (T09) |
| "Everything was a dict at the boundary" | Bodies are annotated; the compile reads the annotations; an unannotated actor is a compile error | flow API (T09) |
| "The collect result was raw rows" | Fan-in bodies receive DECODED results; a skipped child arrives as a typed absorbed item — never silently missing | flow API (T09) |
| "No `drive(until=…)`" | The driver takes `until="held" \| "terminal"` — no hand-rolled dispatch-poll loops | loops (T19) |
| "The ladder laddered everything" | Each node takes `retry=` (the transient/permanent classifier) + `max_attempts` | flow API (T09) |
| "jsonb decoded differently on every path" | Read paths decode once, through the estate's JSON seam | flow API (T09) |
| "No way to read the flow's answer" | The flow result surface: by id, decoded | flow API (T09) |

The two spike-hygiene nits (a vestigial join arg; a module-constant
SCHEMA) have no repo counterpart — the engine carries the parent as a
column, the registry is instance-owned, and the schema is
settings-driven (`TaskQSettings.schema_name`, validated at render).

The full disposition ledger — every cut, its confirmed severity, its
disposition, and the re-test that proves it closed — is
`.measurements/t17-dispositions.md`, THE ONE HOME for the T17 counts
(read the totals there; they are derived from its table, and the
arithmetic lives nowhere else).

## §1 — The flow API: the concept (T09)

`taskq.workflows` now ships the authoring surface: **plain types for
data, `Promise[T]` for wiring**. A workflow is a build function — SYNC
and PURE — that SPELLS the graph by dataflow:

- a promise consumed downstream is an EDGE (the consumer dispatches
  strictly after the producer terminalizes);
- a call with SEVERAL promise arguments is the FAN-IN join — and its
  user body IS that node's body, run inside the join's finalize
  transaction with the DECODED parent results (cut #1's cure: the
  reducer is in the wiring, not engine code; its result cascades
  downstream as NORMAL steps);
- `map_source(source, body)` is the map: the source's finalize forks N
  children (fresh jobs — per-item ledger identity), the join collects,
  and the promise is the FLAT `Promise[list[R]]` (never
  `Promise[list[Promise[R]]]` — the checker rejected the nested shape);
- `route(source, arms)` is THE TYPED ROUTE (T27): the arms are a dict
  keyed by the source's union MEMBERS' TYPES —
  `route(docs, {TextDoc: RouteArm(body=extract_text, queue="cpu"),
  ImageDoc: RouteArm(body=ocr_text, queue="gpu")})` — each element runs
  ITS type's arm as a fresh job, the arm's `RouteArm` stamps its
  children's queue/actor (the per-arm placement), the arm body's
  declared param type is the decode's target (the element arrives AS
  the declared model — never a dict), and the derived `<source>.join`
  packs the arms' returns (the typed sum — the flat `Promise[list[R]]`,
  R the union of the arms' returns). THE ARM CONTRACT IS EXACT (E15's
  exact-member law at `validate()`): the declared
  model must BE the union member — a SUPERCLASS builds clean past the
  old issubclass fence and the runtime decode then drops the
  subclass's own fields (pydantic's `extra='ignore'` default, the
  silent data loss the sum's reader cannot see), and an UNRELATED
  model is the wiring promising data the arm cannot accept; both are
  the build-time refusal. The
  children's rows are graph
  nodes whose step keys NAME their arm
  (`<source>.item:<module.qualname>`), and the join feeds the
  downstream wiring like any node's — FORK, FAN BACK IN at the next
  step (the barrier, §1's barrier section below).
  `map_source(source, {A: fn_a, B: fn_b})` is the same verb's dict
  form — one machinery, two spellings;
  **THE ROUTE ATTACHES TO A WIRED SOURCE NODE** (the LedgerWatch F1
  cure — the constraint stated where the verb is taught, not only in
  the error): a route's source must be a node with a BODY — the route
  dispatches the source's own return. A map's derived join
  (`<source>.join`) declares no body (it is the default identity
  packer), so `route(enriched, arms)` over a map's collect is REFUSED
  at build. The natural enrich-then-branch pipeline spells the
  identity pass-through step between the map and the route — the
  honest one-liner: `enriched = step(branch_seam, mapped, key="branch")`,
  `route(enriched, arms)` (the pass-through's body returns its
  argument; the route's source is then the enriched collect, named).

**THE SINGLE-VALUE DISPOSITION (stated, because the refusal names it)**:
`route(source, arms)` is a MAP face — its source produces the
ELEMENTS' list, and a SINGLE-VALUE (non-list) `Promise[A|B]` is refused
at build (`route's source 'x' does not declare a list[...] return —
the route is a MAP face: the source produces the elements' list, the
arms key its members`). There is no per-value arm dispatch on a plain
step: routing ONE union value is the chain step's in-body `Route`
(the §router below — `Route({EnrichOutcome.OK: "score", …})`, the
total-over-the-outcome-enum fence at declaration time); inside a graph
body, match the union yourself and return the arm's product.
- `sink(...)` is the explicit fire-and-forget (RECORDED in the compiled
  metadata — never silent); `build(result, *residuals)` is the terminal
  completeness point (the `Promise[Never]` residuals are the static
  side of E2's produced-never-consumed rule).

Nothing about the tree is STORED: the graph is spelled by the wiring
and compiled fresh — same module → same compile, byte-stable (the
Mermaid golden's law). Sequencing, hence a depth-N DAG, is dataflow —
cut #2's cure: the migration's depth-3 graph fits ONE flow.

The dispatch-time predicates (cut #4's cure): `step(..., skip=pred)` —
`pred: bool | Callable[[state], bool]` — is evaluated WHEN THE NODE
DISPATCHES against the flow's state (`{"input": …, "results": {…}}`), a
sibling's COMPLETED result decides it. A skipped child SUCCEEDS WITH
THE RECORD (the result names the skip — the envelope never lies about
what ran) and fans into its absorbing joins (collect | maybe) as a
typed item — a skip is not an attempt (zero ledger rows).

**DEPRECATED FOR DISPATCH — teach the typed route.** The predicate's
face is STRINGLY: the state it reads is decoded JSON dicts, and a
predicate keyed by a tag (`item["mime"] == "video"`) can skip EVERY
downstream arm and still let the run terminalize SUCCEEDED having
routed NOTHING — the exact silent drop the totality fence exists to
refuse. For
"dispatch each element by what it IS", the typed route is the taught
face: the union's members key the arms, a non-total route is a BUILD
refusal (E15), and an element of a type with no arm dies LOUDLY
(`RouterNotTotal`) — never succeeds-having-routed-nothing. The
predicate stays for the sibling-conditional shapes it is honest for
(an "only if X succeeded" dispatch), and its docstring names the
conviction.

### The sync barrier — fork, fan back in at the join (T27)

The typed route's arms each emit per-element results; the next step
consuming the route's promise is the FAN-IN — the worked shape (the
document sync pipeline, `examples/doc_mime_route.py`, verbatim):

```python no-exec — not executed: fragment, the author's bodies (sync_source, extract_text, ocr_text, dead_letter, chunk, enrich) are the example's own
docs = step(sync_source, key="docs")
routed = route(
    docs,
    {
        TextDoc: RouteArm(body=extract_text, queue="cpu"),
        ImageDoc: RouteArm(body=ocr_text, queue="gpu"),
        UnsupportedDoc: RouteArm(body=dead_letter),
    },
)
chunks = step(chunk, routed, key="chunk")  # the chunk consumes the join's collected list
return build(step(enrich, chunks, key="enrich"))
```

The barrier's semantics, stated exactly:

* **THE ALL-MEMBERS JOIN** — the chunk fires ONCE, only when ALL the
  routed children have terminalized (the join's `deps_pending` count =
  the elements). The staggered arms (the text fast, the OCR slow)
  cannot make the chunk fire early or twice: the fire ledger
  (`wf_join_fire`, `UNIQUE(join_job_id)`) carries at most one fire per
  joined node, ever, and the chunk's claim starts strictly after the
  LAST child's terminal. THE EMPTY CORPUS IS TRUTHFUL (the born-zero
  receipt — §the build-time validators): a source whose body returns
  `[]` forks zero children, the join fires immediately with the EMPTY
  list, and the run terminalizes SUCCEEDED — `result() == []` is the
  typed sum's honest value, never a wedge and never a lying `None`.
* **THE DECODE AT THE BARRIER** — the chunk body's list param is the
  decode's target: the collected list re-validates into the DECLARED
  models (the typed sum — `list[ExtractedText | DeadLettered]` here);
  the body sees models with attribute access, never dicts. A foreign
  item in the sum dies LOUDLY in the decode.
* **THE FAILURE FACES** — the route's `on_failure=` declares the join's
  incoming edges' policy (T06/T07's duality, at the route's fan-in):
  - `fail_closed` (the default): a child's terminal failure → the join
    blocks with `blocking_reason='failed_parent'` NAMING the failed
    parent on the row, the chunk never fires, and the flow fails (the
    cascade) — the partial result never masquerades as the whole.
  - `collect` | `maybe`: the child's failure fans in as a typed
    `FailureInfo` item on the join's `metadata.failures` (the policy
    that ran is ON the item — the envelope must not lie) + the
    decrement; the join fires when the last child terminalizes, with
    the SURVIVORS ONLY packed (the absorbed parent contributes NO
    result item to the sum — its absence is on the record, never a
    junk slot the decode cannot honor), and the flow lives (the
    derivation reads the absorption record, never a heuristic).

The typed boundary (cut #8's cure): the body's param annotations are
the payload codec — the runner re-validates the jsonb round-trip into
the DECLARED model; a body sees the type it declared, never a raw
dict. `wf.validate()` refuses an unannotated body (E4 — the annotation
IS the wiring).

**THE UNION CONSUMER'S DISCRIMINATOR LAW (E16)**: a body
param decoded as a UNION of models (the join's downstream reads
`list[ImageResult | AudioResult]`) is decoded by the codec's
left-to-right smart union — the FIRST member whose required fields the
payload satisfies WINS. When two members' required fields stand in the
subset relation, the richer member's payload satisfies the thinner
contract and the decode picks the first-annotated member — the richer
member's own fields silently never surface (the mispick).
`validate()` walks every body the runner injects into (the steps, the
map's item children, the route's arms, the loop bodies) and names the
risk by the members' declared fields — the only evidence the compile
has: FIELD-IDENTICAL required faces are the hard refusal (the mispick
is GUARANTEED — every payload decodes as the first member); the
one-directional subset is the warning naming it; distinct members
(each requires a field the other lacks) are clean — the thinner
validation fails on the missing field, the union falls through to the
true member. The fix either way: a `Literal` discriminator field on
each member (`kind: Literal["image"]` / `Literal["audio"]`) — the
discriminated union decodes by the tag, never by field-count luck.

**THE TYPED EARLY-EXIT — `Exit[DoneT]`** (§17.1, the None-end's
sanctioned escape): a body returns `Exit(payload)` to TERMINAL-SUCCEED
the node with a typed result NOW — the runner unwraps the sentinel, the
node's ledger terminal records the exit, and every non-terminal
DOWNSTREAM node is marked SKIPPED-WITH-THE-RECORD
(`{"skipped": true, "exit_from": …}` — the record never lies about the
nodes that didn't get to run; zero ledger rows, a skip is not an
attempt). All rows terminal → the derivation reports the workflow
COMPLETE. A plain `T` return is the DATA result — sentinels appear only
where the body's control flow says so; a body annotated `-> Exit[Report]`
returning a bare value (or a bare `return`) is the checkers' MUST_ERROR.

**THE MANUAL RESUME — `FlowRunner.retry_node(run_id, node_key)`** (§17.2,
the §22.6 redispatch row 3 — "the ladder then manual"): the operator's
audited, CAS-guarded re-arm of a TERMINAL-FAILED node. The attempt
ordinal CONTINUES (never resets — the ladder's own 'failed' count is
the budget, so each manual retry buys exactly ONE more attempt); the
re-run is safe by the step ledger (`ctx.substep` returns recorded
results); the cascade's blocked closure re-opens (the stamps return to
join-wait — the sweep's re-derive re-derives them, a stamp is the
cache); a terminal-FAILED flow root returns to `running` (a CANCELLED
root stays closed — the cancel was deliberate). "Who retried this" is a
ROW (`workflow.retry_node`). Surfaced as the `taskq flows retry` verb
(T12).

The retry knob (cut #12's cure): `step(..., retry_kind=..., max_attempts=...)`
— `retry_kind="permanent"` takes NO ladder; a transient failure's
attempts re-pend with backoff and emit NO terminal until exhaustion
(P3 rule 7), then T06's propagation takes over.

### THE CONTROL-FLOW SIGNALS (the same typed signals the vanilla bodies raise)

A workflow body raising `taskq.exceptions.Snooze` or `RetryAfter` does
NOT ladder — the signals are the body's control flow, never a failure —
and the runner resolves each to a NAMED disposition, the wf face of the
vanilla consumer's own catch (the row dispositions `mark_snoozed` /
`mark_retry_after_consume_{true,false}`, mirrored onto the finalize's
fences):

| signal | the disposition's row law |
| --- | --- |
| `Snooze(delay)` | the node re-schedules at `clock_timestamp() + delay` (floored at `MIN_DEFERRAL_INTERVAL` — a zero/now snooze must not park at the head of the dispatch order); the claim's attempt increment is **REFUNDED**, so `max_attempts` is never exhausted by snoozes alone and the ledger re-claims the SAME attempt arbiter row; `snooze_count` increments; no terminal, no decrement — the join's accounting is unchanged and the run stays live. |
| `RetryAfter(delay, consume_budget=False)` | the same snooze semantics (the vanilla's own vocabulary) — the refund, the floor, the counter. |
| `RetryAfter(delay, consume_budget=True)` | the claim's increment **STANDS** (the burn — the next claim mints the next attempt), the raw delay is kept, and the budget gates it: past `max_attempts` the step terminal-fails `MaxAttemptsExceeded` ("retry budget exhausted") through the wf terminal door — the T06 propagation takes over. |

In every case the step's result capture stays empty (nothing succeeded),
the claim's ledger row rests `awaited` (the hold's own
resume-not-retry status — never a `failed` row the ladder would count),
and the node's metadata carries the deferral stamp — the run page's
[deferral line](#visualizing-workflows-the-admins-run-explorer) renders
the signal, the next fire time, and the snooze count while the step
sits. The signals' full vanilla contract (the construction-time
`ValueError`s, the `schedule_to_close` interaction, the
`Retry-After`-header example) is [Control-flow
signals](retries.md#9-control-flow-signals). A deferral refused by an
operator cancel in flight (the cancel mark wins, the vanilla's own law —
on EITHER carrier: the workflow cancel's metadata stamp AND the vanilla
cancel's own column mark, the admin job-page cancel's arm)
terminalises the node `cancelled`, and a zombie's deferral (the claim
fenced) records the attempt `fenced` — the run is never wedged by a
refused deferral. The extension seam is named, with its honest bound:
the next deferral DISPOSITION (a new signal, a non-terminal row law)
adds a `DeferralDisposition` member, one statement arm, one resolver
entry — and nothing else; a new TERMINAL verdict (a death the doors
own) additionally needs a per-door route in BOTH the step's and the
loop's disposition routes (the doors' semantics differ by law — the
step's exhaustion finalizes through the wf terminal door, the loop's
through the loop's own named state — so the route is per-door, never a
shared special case). The disposition's seat is the
runner's step-body seam (the step, the chain, the map's item children,
the route's arms); a LOOP body raising a signal routes through the SAME
disposition — the loop's iteration machinery already resumes a
mid-iteration hold, and the deferral is the same resume, later by the
delay: the loop node row re-schedules, the iteration counters are
untouched (the advance never ran), and the loop resumes at THAT
iteration with the ladder unburned. The bound holds there too: a loop
body exhausting the budget via `RetryAfter(consume_budget=True)` past
`max_attempts` takes the loop's own body-failure face (the named state +
the flow terminalized, `MaxAttemptsExceeded` on the record) — the ladder
still bounds loops.

THE DEADLINE INTEROP (the step's own `start_to_close` — the resolution
law in the [capacity surface](#the-capacity-surface-the-declared-admission-terms)
table): a `Snooze`/`RetryAfter` raised WITHIN the window defers (the
deferral wins the enforcement's race — the deadline never fires); a
START_TO_CLOSE expiry is a REAL attempt burn, never a deferral, so a
body catching the deadline's cancellation and converting it to a
`Snooze` LOSES — the clock's win is final (the vanilla enforcement's
own race, mirrored verbatim); and an operator cancel in flight outranks
the deadline (the cancel-fenced law — the node terminalises
`cancelled`, the cancel's vocabulary).

The reads (cuts #14/#19's cures): `FlowRunner.result(flow_id)` — the
terminal's result, DECODED ONCE through the estate's JSON seam; on a
FAILED run the read is the FAILURE's face — the typed
`WorkflowRunError` carrying the failing node row's error class +
message (WHAT failed, and why, answered AT the read; failed and
running are never indistinguishable). The driver
`drive(flow_id, until="held" | "terminal")` (cut #10's cure —
landed with T19's loop; the bound `max_ticks` is the hang's fence).

**THE DRIVE'S RETURN IS THE VERDICT — CHECK IT.** `drive` returns one
of three strings: `"terminal"` (the flow row reached
`succeeded`/`failed`/`cancelled`), `"held"` (only under
`until="held"` — a node waits on a human/deadline), or `"max_ticks"`
(the tick budget was SPENT without reaching `until` — a bounded
give-up, NOT a status). A `"max_ticks"` return is the driver saying "I
stopped trying"; the operator's next act is to read the ROWS
(`taskq flows status`, the flow root's status) to learn why — never to
assume the run advanced. **RESUME SEMANTICS — the crashed in-process
run is the one door that does not reopen in-process**: the in-process
drive runs no lock-reclaim sweep (that is the worker maintenance
leader's job), so a run whose executing process died keeps its
`running` rows leased by a dead worker, and re-driving
`drive(execute=True)` on a fresh runner ticks to `max_ticks` (~110 s
at the defaults) changing nothing. The working resume is the FLEET
door: `drive(flow_id, execute=False)` (or just the worker fleet) — the
leader's lease-reclaim sweep waits out the dead claim's lock lease
(~2 min at the defaults; see the runbooks' mid-run crash page), re-pends
the killed child, and the fleet executes the rest. The cancel +
re-issue runbook (a fresh `run_key`) is the alternative when the
re-drive is not wanted — the idempotent absorbs make the re-issue's
re-execution safe.

## §2 — The type contract (T01's two tables)

The wiring's typing story has TWO faces, and both are load-bearing:

1. **The checker face** (static): the plain-function bodies are
   checker-typed end to end; the negative probes live in
   `tests/typeprobe/` (pyright 1.1.414 + ty 0.0.85, the CI
   `type-probes` gate). The checker is ABSENT at runtime — which is
   exactly why the second face exists.
2. **The validator face** (runtime, checker-independent): `wf.validate()`
   re-proves the wiring's totality from the compiled graph (the rules
   table in the API reference). A graph that typechecks AND validates
   is pinned clean by BOTH; a mutation of either face reds its own pin.

### THE TWO FACES — what each one owns (the honest boundary)

The mechanism shipped in the verbs is probe-proven (the type-mechanism
corpus, `wf_generic_step_negative_types.py`, on both pinned checkers):
`Promise` is COVARIANT; `step[R]` infers `R` from the body's declared
return (`step(fetch, params)` wires a `Promise[Report]` when `fetch`
returns `Report`); `gather[R]` preserves the element type
(`Promise[list[R]]`, never an erase to `Promise[Any]`); `build`'s
residual slot is `Promise[Never]` — covariant, so every REAL data
handle in the slot reds. **But the two faces own different questions,
and neither covers the other's:**

| the edge's question | WHO catches it | WHERE |
| --- | --- | --- |
| the handle flow: an unconsumed data handle passed to `build` as a residual (`Promise[Report]` in the `Promise[Never]` slot) | **THE CHECKER, at typing** | `build(...)` call — red in the IDE, red in CI (`reportArgumentType` / `invalid-argument-type`) |
| the handle flow: a consumer that DECLARES the handle type (`Promise[Config]`) handed the wrong handle (`Promise[Report]`) — the contravariant-consumer rejection | **THE CHECKER, at typing** (live because `Promise` is covariant) | the handle-passing call site |
| the handle flow: `gather`'s element type threaded to a typed join consumer (`Promise[list[Report]]` where `Promise[list[Config]]` is declared) | **THE CHECKER, at typing** | the join-consumer call site |
| the promise-as-data mistake (a promise passed where the DECODED model / list is wanted — the direct unit-call form) | **THE CHECKER, at typing** (the corpus's `wf_api_negative_types.py`) | the unit-call site |
| the DECODED-payload compat: a producer's body returns `Report`, a consumer's body declares `Config` | **`validate()`, at build** — `E5-incompatible-consumer` | `compiled.validate()` / worker boot |
| the DECODED-payload hole: the consumer's param carries NO model annotation (`Any` / a plain dict — the duck-shaped hole) | **`validate()`, at build** — `E5-incompatible-consumer` | `compiled.validate()` / worker boot |
| a promise nobody consumes, nobody sunk, nobody named terminal (not passed to `build` at all) | **`validate()`, at build** — `E2-produced-never-consumed` | `compiled.validate()` / worker boot |

The boundary has a REASON, not just a history: the bodies take DECODED
payloads, not handles (cut #14's decode-once — the fan-in's args are the
parents' decoded results). The wiring verbs' `*args` are therefore
ERASED (`object`) — statically they are either promise handles (the
handle flow, the checker's face) or plain data, and the checker cannot
see a decoded model inside an erased argument list. That is why the
canonical wrong graph "producer `Report` → consumer wanting `Config`"
red at the WIRING SITE (`step(consume, produced_promise)`) is NOT a
checker error — the wiring call is erased — and IS a validate() refusal
(`E5`) the moment the graph compiles. The corpus asserts this boundary
as its own probe (`probe_wiring_site_decoded_payload_is_the_validators_face`):
the line must stay CLEAN under both checkers, and the gate's
unmarked-error rule reds the corpus if it ever reddens. What the IDE
catches at typing is the HANDLE flow; what `validate()` catches at build
is the DECODED-payload compat. Read the gate's red as the handle-flow
law; read validate()'s refusal as the payload law; neither report
subsumes the other.

## §3 — Fan-out & reduce: the one-flow shape

The common pipeline — fan-out N independent steps, reduce into a join,
cascade downstream — is spelled (never glued):

```python
from pydantic import BaseModel

from taskq.workflows import Promise, StepContext, WorkflowApp, build, step

app = WorkflowApp()


class Ingest(BaseModel):
    doc_id: str


class Stats(BaseModel):
    n: int


@app.actor(queue="cpu")
async def stage_a(ctx: StepContext, params: Ingest) -> Stats:
    return Stats(n=1)


@app.actor(queue="cpu")
async def stage_b(ctx: StepContext, params: Ingest) -> Stats:
    return Stats(n=2)


@app.actor(queue="cpu")
async def reduce(
    ctx: StepContext, a: Stats, b: Stats
) -> Stats:  # the join's user body — the DECODED parents
    return Stats(n=a.n + b.n)


@app.actor(queue="cpu")
async def tail(ctx: StepContext, total: Stats) -> Stats:
    return total


@app.workflow("doc_ingest")
def doc_ingest() -> Promise[object]:
    a = step(stage_a, Ingest(doc_id="d1"), key="a")
    b = step(stage_b, Ingest(doc_id="d1"), key="b")
    reducer = step(reduce, a, b, key="reducer")  # the fan-in: TWO parents
    return build(step(tail, reducer, key="tail"))  # the cascade: a NORMAL step


compiled = app.get("doc_ingest")
compiled.validate()  # the zero-false-positive bar: this graph is clean
```

The fan-in's failure policy is declared ON the join
(`on_failure="fail_closed"` — T06's duality: a failed
parent either fails the join closed (the cascade, the peers
peer-cancelled) or fans in as a typed `FailureInfo` item and the join
FIRES with the typed partial). The ABSORBING policy has two accepted
spellings — `"collect"` (T06's shape: the failures fan in AS PART of the
typed partial) and `"maybe"` (T07's shape: the branch's contribution may
be absent) — the engine treats both as the same absorption (the typed
`FailureInfo` item + the fire with the partials); pick the name that
reads honestly at the call site. The fan-in bound is 1000 declared
parents (`MAX_FAN_IN_PER_JOIN`) — past it, partition the map.

For a RUNTIME-determined N: `map_source(source, item_body)` — the
source's body returns the list, the fork spawns one fresh job per item
(map_index = the item's ledger identity), and the join packs the
decoded item results for the downstream body. The map takes NO `key=`:
the join's key is DERIVED (`<source-key>.join` — the engine's fork
addresses it), so naming the node means naming the SOURCE. (A `key=`
parameter on the map does not exist and no value of one would be
honored — the derived key is the only form.)

**THE ITEM LADDER HONORS THE STAMPED `max_attempts`** (the LedgerWatch
F3 cure — the knob is the operator's, the record keeps it): a
transient-failing item ladders to the `max_attempts` wired on the
`map_source` — the SAME knob a plain `step` honors, stamped on every
child row by the fork. The pre-cure ladder capped items at the bare 3
regardless (the row LIED — a plain step wired 7 laddered
7; a map item wired 7 died at 3 — and every adoption
re-implemented the ladder client-side). The deterministic-failure and
`permanent` routes are unchanged (they never ride the ladder).

**THE LADDER'S BACKOFF CURVE** (the LedgerWatch F5 cure — the plan-ahead
numbers): a laddered attempt's re-pend waits
`0.05 × 2^(failed_count)` seconds — 50 ms after the first failure, then
100 ms, 200 ms, 400 ms, 800 ms, 1.6 s, 3.2 s… doubling with no cap
inside the ladder (the vanilla actors' retry curve has its own
documented jitter and caps — `docs/guides/retries.md`; the workflow
node's ladder is this plain doubling). A drill's worst-case wall
budget for `max_attempts=N` is therefore the sum of the first N−1
doublings plus each attempt's own run time.

## §3b — Child flows: a step spawns a sub-flow and joins its result (T28)

The map's fork fans out inside ONE flow. When the sub-work is another
WHOLE flow — the agent spawning sub-investigations, each a real run the
run explorer sees, the admin operates, the CLI lists — the step calls
`ctx.spawn_flow` and JOINS the child's terminal. The child is a REAL
flow: its own root row, its own node rows, its own run-key claim — the
spawn is the child's `create_flow` in the create seam's own ONE
transaction. The join is the HITL hold's own shape: the wait registers
a hold on the spawning node, the slot releases (never a failed
attempt), and the child's terminal is the hold's RESOLVER — the wake
arm delivers, the body resumes, the closed union's member returns.
`timeout_s=0.0` would never wait; the example passes `None` (the
default — wait as long as the child lives; a cancelled parent cancels
its children, so no join outlives its flow):

```python
import asyncio
import os

import asyncpg
from pydantic import BaseModel

import taskq.migrate
from taskq.workflows import (
    ChildCancelled,
    ChildFailed,
    ChildOk,
    Expired,
    Promise,
    StepContext,
    WorkflowApp,
    build,
    step,
)


class Probe(BaseModel):
    topic: str


class SubReport(BaseModel):
    findings: list[str] = []


class Digest(BaseModel):
    verdict: str
    findings: list[str] = []


child_app = WorkflowApp()


@child_app.actor(queue="default", max_attempts=1)
async def sub_investigate(ctx: StepContext) -> SubReport:
    # The FLOW INPUT's face: the spawn's `input=` rides the child
    # root's payload; the body re-hydrates it through the declared
    # model's own validator (the jsonb value, decoded once).
    probe = Probe.model_validate(ctx.input)
    return SubReport(findings=[f"looked at {probe.topic}"])


@child_app.workflow("docs_childflow_child")
def docs_childflow_child() -> Promise[object]:
    return build(step(sub_investigate, key="probe"))


parent_app = WorkflowApp()


@parent_app.actor(queue="default", max_attempts=3)
async def investigate(ctx: StepContext, params: Probe) -> Digest:
    # THE SPAWN: the child is a REAL flow. The run key
    # (`childflow:{parent_flow}:{node}:{slot}`) is the TWO-CLAIMS LAW
    # at the flow level: the retry ADOPTS the same child, never
    # duplicates it (a terminal child adopted as-is is the
    # deterministic replay; `key=` overrides the slot).
    handle = await ctx.spawn_flow(
        "docs_childflow_child",
        input=Probe(topic=params.topic),
        result=SubReport,
    )
    # THE JOIN (the wait IS the hold): the CLOSED union — the checker
    # forces the fail-close arms (a body matching only ChildOk falls
    # through and reds against this Digest return).
    outcome = await handle.wait()
    match outcome:
        case ChildOk(result=report):
            return Digest(verdict="ok", findings=report.findings)
        case ChildFailed():
            # THE CHILD FAILED — never a silent swallow: the body
            # DECIDES (finish with what it has here).
            return Digest(verdict="child-failed")
        case ChildCancelled():
            return Digest(verdict="child-cancelled")
        case Expired():
            # THE JOIN'S OWN PATIENCE EXPIRED (`timeout_s=` arms the
            # hold's deadline — the DB clock; the face fires ONCE per
            # abandoned hold per attempt).
            return Digest(verdict="expired")


@parent_app.workflow("docs_childflow_parent")
def docs_childflow_parent() -> Promise[object]:
    return build(step(investigate, Probe(topic="the-estate"), key="spawn_join"))


async def main() -> None:
    dsn = os.environ["TASKQ_PG_DSN"]
    schema = os.environ.get("TASKQ_SCHEMA_NAME", "taskq")
    await taskq.migrate.apply_pending_locked(dsn, schema=schema, phase="pre")
    await taskq.migrate.apply_pending_locked(dsn, schema=schema, phase="post")
    pool = await asyncpg.create_pool(dsn)

    from taskq.workflows import FlowRunner

    # The registration door — the spawn resolves the child's compiled
    # graph from the registry (the definitions must be imported in the
    # driver's process).
    child_app.get("docs_childflow_child")  # noqa: B018  (registers the child)
    runner = FlowRunner(parent_app.get("docs_childflow_parent"), pool, schema)
    claim = await runner.create_flow(input=Probe(topic="the-estate"))
    outcome = await runner.drive(claim.flow_id, until="terminal")
    assert outcome == "terminal", outcome
    result = Digest.model_validate(await runner.result(claim.flow_id))
    assert result.verdict == "ok", result
    assert result.findings == ["looked at the-estate"], result
    await pool.close()


asyncio.run(main())
```

The identity runs BOTH directions: the parent's node row carries the
child's flow id (`metadata.child_flows` — the receipt), the child's
root row carries the parent's (`parent_flow_id` / `parent_node_id` /
`parent_step_key` — the provenance). The cancel cascade: cancelling the
parent's run walks the family from the rows' parent links (the
receipts the accelerator) and cancels each child's rows — the worklist
walk below. The fan-out is the body's own loop: spawn K handles (each
slot a distinct, retry-stable key) and join each — the fork's
all-members semantics at the flow level.

**THE SPAWN'S ATOMICITY FACE (the gather dragon's cure).** The default
slot is CLAIMED in one atomic statement (the per-attempt cursor's
increment and the returned slot are one row-CAS), so K CONCURRENT
spawns — `asyncio.gather(*[ctx.spawn_flow(...) for t in tenants])`, the
natural fan-out spelling — take K DISTINCT slots and mint K DISTINCT
children, each carrying its own input. The pre-cure shape raced the
cursor's read and write: all K spawns adopted the same slot, minted ONE
child, and every caller received tenant-0's verdict wearing K labels —
silent fabricated results, zero errors, green pins. A SAME-key
concurrent spawn ADOPTS the same child (the arbiter's run-key CAS) and
every caller receives THAT child's handle — the honest shared face.
One discipline applies: a concurrent fan-out that must join STABLY
across the resume passes `key=` (the slot's allocation order is the
scheduler's; the author's identity is the caller's — a resumed body
re-executes the gather and the adoption follows the KEYS).

**THE DEPTH BOUND.** A self-spawning flow (a workflow that spawns
itself, directly or through a cycle of children) cannot mint a
generation per sweep tick forever: the spawn walks the ancestor chain
from the ROWS' `parent_flow_id` links — capped at the declared max
depth, `DEFAULT_SPAWN_MAX_DEPTH` (32) — and past the bound the typed
`SpawnDepthExceededError` (`FanInBoundExceededError`'s sibling)
terminal-fails the node, the retry ladder unburned (re-running cannot
shrink the ancestor chain). The same walk detects a CYCLE in the parent
links (a corrupted/operator-written link cannot run the walk forever —
the same bound fires, the message names it). A fan-out that genuinely
needs more depth declares `max_depth=` at the spawn site.

**THE RETENTION INTERPLAY (the operator's law).** The cancel cascade
cancels the family; the retention purge must WALK THE PARENT LINKS the
same way — purging a child's rows while its parent still runs leaves a
DANGLING child pointer on the parent's row. The parent's resume now
FAILS CLOSE on it: the dangling pointer raises the typed
`ChildFlowLostError` and the run terminal-fails with the class on the
record. It is NEVER a silent respawn — the pre-cure shape re-minted a
fresh child on the resume (the purge took the arbiter's row with it),
the child's side effects ran twice, and the parent SUCCEEDED.

**THE CASCADE WALKS THE ROWS.** The cancel cascade collects children
from the ROWS' crash-safe `parent_flow_id` direction first — the
receipts (`metadata.child_flows`) are the accelerator, the rows the
law. A child created but never receipted (the crash between
child-create and receipt-write) is STILL collected: the orphan window
— the crash-then-cancel that let a child escape the cascade forever —
is unrepresentable. The cascade itself is a WORKLIST WALK, never
Python recursion: a 1200-deep family cancels clean (the recursive walk
raised `RecursionError` at the operator and left hundreds of runs
running).

**THE DEV-DRIVES PARITY.** The dev-loop driver's tick runs the expiry
arm, so a `timeout_s` wait — including a child join's — fires the
`Expired` member in a dev drive exactly as it does under the leader's
sweep belt. The pre-cure dev face parked on the hold until
`max_ticks`: the deadline inert, the dev loop a different engine from
the fleet's.

**THE ADMIN SEES THE FAMILY.** The run explorer renders the
parent↔child links the rows carry: the parent's page links each child
flow's run page, a child's page links its parent — the identity faces
the child line landed, surfaced.

## §9 — Loops & back-edges (T19)

`wf.loop(name, body, *, initial, carry_type, until, max_iterations,
budget_s, on_exhausted, escalates_to, gates)` — the loop is v1; the
signature is the SHIPPED verb's own, every parameter spelled: `initial` seeds the
carry at iteration 0, `carry_type` declares the carry's model
(revalidated at every boundary), `until` is awaited per iteration,
`budget_s` bounds the loop's wall clock, and the exhaustion takes
`on_exhausted` (`"fail"` | `"escalate"` — the escalation step via
`escalates_to=`, with `gates=`'s HOLD gates). Each iteration is FRESH jobs: the iteration-scoped
step keys `(workflow, loop_key, iteration, step)` keep the idempotency
ledger per-iteration (T05's contract unchanged). The body returns the
CONTROL UNION:

- `Done(payload)` — the loop stops; the payload is the loop's result.
- `Refine(feedback)` — the carry threads into the next iteration.

**THE CARRY IS FROZEN AT SPAWN** and advanced EXACTLY ONCE per
iteration — in the ADVANCE STATEMENT, one atomic write shared with THE
CAP GUARD (a refused advance IS the exhaustion). A carry advanced at
hold/retry time is the optimistic-apply dragon (double-apply /
lost-apply) — the pin keeps it red forever. Crash recovery: a resume
reads the iteration's LEDGER row (the memo) — the body is not
re-consulted mid-iteration without the at-least-once boundary being
stated.

**THE TWO WALLS ARE DIFFERENT** (the spike's cut 4):

- the **iteration cap** (`max_iterations`) bounds TOTAL SPAWNS
  regardless of time — the only wall left when the budget is paused;
- the **budget** (`budget_s`) is the TIME wall — and it is BLIND while
  the loop holds on a human (`budget_paused`): a held loop is invisible
  to the budget sweep even when its deadline is forced into the past.
  THE CONSUME-BUDGET DRAGON (the consume alternative — the arm reads
  the budget instead of pausing) killed a held loop mid-hold and
  refused the operator's later approval: work silently lost. It is kept
  RED forever by the mutation drill (`.measurements/t19-pin-reds.json`).
  (The operator's OWN latency between two holds, though — that time the
  budget DOES consume once the pause lifts: the wall measures the loop's
  working time including the gaps, never the holds themselves.)

**EXHAUSTION IS NAMED, NEVER SILENT**: the cap or the budget wall
terminates the loop into the `iteration_cap_exhausted` /
`budget_exhausted` state — and THE FLOW TERMINALIZES IN THE SAME
TRANSACTION (STRANDED-FLOW: the spike's `_loop_advance` left the flow
`running` forever — the worker ticks forever, the admin shows a live
run; the pin convicts it).

**THE EXHAUSTION POLICY IS READ — BY THE DRIVER AND THE SWEEP**:
`on_exhausted="escalate"` enqueues the escalation
through the SAME outbox the fired joins use — addressed to the
workflow's REGISTERED escalation step (`loop.escalation`; the body is
your `escalates_to=fn`, or the framework default, registered at compile
by the definition registry), and the escalation consumer job RUNS:
the drained job resolves the registered body and terminal-succeeds
carrying the exhaustion record — no dead letter, on EITHER path (the
live driver's cap check AND the sweep's orphaned-loop arm). (The
payload door — the escalation's payload — is matched to the registered
escalation step's declared shape, never to the loop's argument order:
the wiring's SHAPE decides, the declaration order decides nothing.)
`on_exhausted="fail"` terminal-fails the flow and enqueues NOTHING.

**THE CAP LETS THE FINAL ITERATION RUN**: the advance guard admits the advance TO the cap, so the
metadata's counter REACHES `max_iterations` — the live driver exhausts
at its top-of-loop cap check, and the crash window (a worker death
after the final advance) leaves exactly that state on the running row
for the SWEEP's cap arm to own. The sweep's
`iteration >= max_iterations` predicate is REACHABLE in production —
never a hand-crafted state.

**THE SEMANTICS DECISION, STATED ONCE — BY ESCAPE POINT**: infra fault
≠ body failure, and the classifier reads WHERE the error escaped, never
merely its type. An exception the BODY raised is a BODY failure even
when it wears a `ConnectionError` face — the loop's typed failure
(`LoopBodyFailure`, the flow terminalized); a body cannot forge an
infra fault (the poison-body wedge — 20 crashed rows, a `running` flow
forever — is the convicted variant, kept red by the attack probe). An
infra fault raised by the DRIVER'S OWN machinery reclaims: the ledger
says `crashed`, the ladder does NOT burn, the lease machinery re-claims
from the ledger, the loop completes on the re-claim.

**`until=` is AWAITED** (`Callable[[], Awaitable[bool]]`): a bare sync
closure returning a coroutine object is TRUTHY — the spike's cancel
test cancelled an IDLE flow, proving nothing. The predicate is awaited
by the driver, per iteration.

**NAIVE-MEMO IS AUTHOR GUIDANCE, PINNED** (the spike's cut 7): the
durable memo — one model invocation per iteration, never one per
resume — is the ONE-TX shape (read + write inside one transaction; the
ledger's claim statements ARE that shape). The engine gives you the
carry + the TX boundary; the memo shape is author work — the negative
example (a read-modify-write across transactions: a crash between
"model ran" and "proposal committed" strands the counter, the re-run
UniqueViolations, the ladder then fails the loop) is the shape to
never write.

The driver (`drive(flow_id, until="held" | "terminal")`, cut #10's
cure) is bounded — `max_ticks` fences the hang.

## §10 — The streaming source & the chain (T20)

A "source sync" — a paged upstream whose run executes a body that is a
**paged generator**: fetch page → `ctx.emit_batch(children, cursor=…)`
→ checkpoint the page cursor → fetch the next page. Pages stream: each
page's records are **chain-start rows** that claim and terminalize WHILE
the source is still mid-stream (the spike measured 236 chain claims +
165 chain terminals before the last page's emit) — nothing waits for
the pager to exhaust.

### The emit tx — the one new primitive

`ctx.emit_batch(children, cursor=…)` is ONE transaction:

1. the **children INSERT** — the certified fork-children shape
   (parallel-array, app-side uuid7 ids), parent = the source row;
2. the **edge rows** — child → source, `fail_closed` (a mid-stream
   source death is the honest fail-closed parent; the edges reconcile
   emit debt exactly as the fork's reconcile fork debt);
3. the **cursor checkpoint** — the source row's OWN metadata (the
   `emit_cursor` key; no new table, no new column), guarded by the FULL
   dispatch fence (`status='running' AND locked_by_worker AND attempt
   AND claim_epoch`).

**Why one tx** (the fork-atomicity law at page granularity): a kill at
ANY statement window — including *after* the cursor write but *before*
the commit — rolls the whole batch back; the REAL reclaim
(`sweep_expired_locks`) + the promotion sweep re-pend the source (it
never finalized), and the re-claim re-emits exactly the lost page. The
cursor is never advanced outside the emit tx. The pins kill the emit at
every window with a real `pg_terminate_backend` and count: zero
re-emitted children, zero lost children, the cursor == the last
committed page.

**THE TERMINAL-CRASH CLASS IS TERMINAL-FOR-REAL** (the §17.5 no-zombie
promise's crashed-terminal wedge cure): a node row in `crashed`
(budget-exhausted — the reclaim's crashed branch wrote it because
nothing would ever revive it) or `abandoned` derives the FAILED-class
terminal, never liveness. The runbook asymmetry: a corpse that reports
`running` forever is the wedge (the operator must notice forever); an
honest failed terminal is resumable-by-rerun. The wedged root heals on
ONE maintenance pass — the pin family
`tests/test_wf_t20_crashed_terminal_wedge.py` holds the semantics, the
red-drill (the assertion can red a corpse), and the zombie audit's
spellings. The LEDGER's `crashed` rows are the ATTEMPT-level reclaim
receipt (the row re-pends — the real crash recovery), never a terminal.

**The fence needs nothing new**: with no fan-in there is no join to
fire early — the successor hazard is the PREMATURE TERMINAL (the run
finalizing while work is live), and the fence for it is the shipped
rows-only derivation itself: the root finalizes only when every row is
terminal (or resolved-blocked). A derivation cannot fire early because
it is not a fire at all. The 30-sweep probe (a mid-stream death, then
30 hard sweep passes with NO worker) holds: nothing terminalizes,
`firable=0`, zero join-wait rows — the join machinery is inert in this
design.

### THE REFUTED-CLAIM DISCIPLINE: `map_index` is load-bearing

The per-record identity rides **`map_index`, stamped at emit** (with the
record's `trace_id`). The certified fork's idempotency key
(`wf:{flow}:emit:{map_index}:{step}`) and the step-ledger's claim
arbiter (`(flow, step_key, COALESCE(map_index,-1), attempt)`) BOTH
discriminate siblings by it — the spike's first run emitted 200
applications without it and observed **198 UniqueViolations**: every
record's `enrich` child collided onto one row. `EmitChild` therefore
REQUIRES both stamps (a child without them is refused before any row
exists), and `chain_start` carries them forward. Every row of one
record's chain shares its `trace_id` — the operator's drill-down is one
query.

### The router: conditional edges, total or refused

```python no-exec — not executed: fragment, the author's bodies (screen_app, enrich_app, score_app, manual_review) are the workflow's own
CHAIN = Chain(
    name="application-enrichment",
    start="screen",
    steps={
        "screen": Step(
            body=screen_app,
            outcomes=ScreenOutcome,
            route=Route({ScreenOutcome.CLEAN: "enrich", ScreenOutcome.FLAGGED: "manual_review"}),
        ),
        "enrich": Step(
            body=enrich_app,
            outcomes=EnrichOutcome,
            route=Route({EnrichOutcome.OK: "score", EnrichOutcome.SPARSE: DONE}),
        ),
        "score": Step(body=score_app, outcomes=ScoreOutcome, route=Route({ScoreOutcome.OK: DONE})),
        "manual_review": Step(
            body=manual_review,
            outcomes=ReviewOutcome,
            route=Route({ReviewOutcome.APPROVE: DONE, ReviewOutcome.REJECT: DONE}),
        ),
    },
)


# the SOURCE is the paged generator — each yield = ONE emit tx:
@app.workflow("application_sync")
def application_sync() -> Promise[object]:
    return build(chain_source(CHAIN, source_body, key="source"))
```

The bodies are ordinary typed-outcome coroutines
(`async def screen_app(ctx: StepContext, item) -> ScreenOutcome`); the router's
decision is the body's return. Totality is the fence, at two doors:

1. **Declaration time** — `Chain` refuses a route that is not total
   over its step's outcome enum: `chain step 'screen': route is not
   total over ScreenOutcome — missing ['flagged'] … the outcome it
   drops would silently strand a record's chain.` A route to a
   non-step key, and a non-step start, are the same door's refusals.
2. **Run time** — `RouterNotTotal`: an outcome with no arm fails the
   step LOUDLY with `error_class='RouterNotTotal'` — the record names
   the defect; the record's chain visibly dies, never silently drops,
   and the run's derivation cannot claim success over it.

The chain is declared ONCE and instantiates per record through the
certified fork-at-finalize machinery: each chain step's finalize forks
AT MOST ONE child (the route's arm — no fan-in, no join), the record's
payload, `map_index` and trace riding forward.

### The typed route at the GRAPH level (T27)

The chain's route is the per-record stream's router. The GRAPH DSL has
its own face of the same machinery — `route(promise, arms)` (§1's
typed route): the union MEMBERS key the arms, the per-arm placement
rides `RouteArm`, the arm bodies receive the DECODED typed models, and
the children's rows ARE graph nodes feeding the derived join (the
fan-back-in the chain cannot spell). The arm contract is EXACT: each
arm body declares ITS OWN member on the item param — a superclass (or
an unrelated model) is the build refusal (E15's exact-member law, §1).
The totality fence is the same
vocabulary at both levels, at THREE doors: the wiring verb's refusal
(missing/unknown members, NAMED), the validator's
`E15-route-totality` (the checker-independent re-proof from the
compiled graph), and the runtime `RouterNotTotal` — the element of a
type with no arm (the body that lied about its union) fails the source
LOUDLY, nothing routes, the run FAILS. The skip-silent shape is dead
at both levels.

### Chain or DAG? (the one-paragraph decision guide)

**The chain is the per-record stream** — a route arm per outcome, at
most one child per step, no fan-in, no join; pick it when every record
flows ALONG ONE PATH and the arms are the branching. **The DAG is the
shared shape** — `step`/`gather`/`map_source` wiring (§3) with real
fan-in joins: pick it when independent results CONVERGE (a reducer
consuming two parents, a `gather`'s all-upstream join, a map's
collected join). **The typed route (`route`, T27) is the
converge-and-dispatch shape** — a map whose per-element arms DIFFER
(the mime-type router: text to the chunker, images to the OCR) with
the arms fanning back in at the next step; it is a MAP attachment (one
fork per source), not a chain, and not a bare gather. The wrong guess
costs a restructuring, not a
migration: a fan-in spelled as a chain has NOWHERE to put the second
parent (the chain's finalize forks at most one child — the convergence
is unwritable), and a route spelled as a DAG arm drags the whole graph
into the per-record stream where the shared joins never fire. The two
compose — the chain's records fan OUT of a `chain_source` (§10's emit
tx) into any downstream DAG node, and a chain step's body can be the
consumer of an upstream `gather`: the stream is the row source, the
graph is the convergence.

### The partial-success run (the envelope must not lie)

With no fan-in there is no absorbing edge, so the honest run vocabulary
stays the shipped one: the run **`failed`** when any chain failed
(`UnabsorbedNodeFailure` on the root — the alert hook), **`succeeded`**
when none did. The run's REPORT is the read-side rollup over the rows
(per-trace `bool_or(status='failed')` — `{failed_chains: 7, total: 200}`
is one query), never a new engine state. A "partial" derived status
would be a new primitive (a new terminal class, a new render rule across
the explorer/admin/API surface) rejected: the report reads the same
truth from the rows without it. Partial success as an AUTHORED ROUTE is
the ergonomic answer: route the degraded outcome to a fallback step and
the run succeeds with the degraded records visibly on their own route.

### THE MIGRATION PATH: from the actor's ad-hoc children to the chain

The TaskQ-native pattern this replaces — an actor body dispatching its
own child jobs ad hoc (the hand-rolled "spawn a follow-up job per
record, then a counter row to know when they're done") — maps onto the
chain surface:

| the ad-hoc actor pattern | the flow's shape | what it buys |
| --- | --- | --- |
| `ctx`/client child dispatch mid-body | the source's `ctx.emit_batch` per page (the children + edges + cursor, ONE tx) | the fork-atomicity law at page granularity: a crash rolls the page back WHOLE and the resume re-emits exactly the lost page — the ad-hoc pattern's window (children enqueued, cursor lost, or the inverse) is structurally gone |
| the hand-rolled "done counter" row / the polling parent | nothing — the run root derives from the ROWS (the maintenance leg) | no counter to corrupt, no polling job, no wedged "waiting on children" state: the root's terminal IS the derivation |
| ad-hoc retry/backoff per child | the certified retry ladder (the row's own curve) | the ledger carries the full per-attempt history; the reclaim owns crashes |
| the follow-up job's invisible dependency | the chain's declared route (the fork's edge row) | the lineage is QUERYABLE: `trace_id` per record, `map_index` per record, `parent_id` per row — the ad-hoc pattern's children have no lineage and no completion semantics |
| the payload re-marshalled into each child | the record rides the row's payload verbatim (the fork carries it) | the drill-down reads one record's whole chain by its trace |

**THE LINEAGE LAW**: every row a source/chain writes carries
`flow_id` (the run), `trace_id` (the record), `map_index` (the record's
index — the discriminator) and `parent_id` (the emitting row). The
migration BUYS the lineage and the completion semantics and PAYS the
declared-edges discipline: the children are the fork's (an edge row
each, the ledger's truth), the completions are the derivation's — there
is no ad-hoc escape hatch, and a child dispatched outside the fork is a
foreign row no derivation counts. The honest boundary: an actor's
ad-hoc children that must remain in the NATIVE queue (no flow) keep
their no-lineage semantics — the migration is opt-in per workload, not
a fleet conversion.

The bands (measured on the built code, the spike's §7 protocol): the
emit tx's p50 stays single-digit ms per page (3.2–8.1 ms across the
five captured runs; the max TAIL reaches 20.6 ms on the loaded-box
capture — the artifacts' own range, the figures' truth), and the
chain-shaped dispatch backlog costs the same ms-class as plain jobs at
200 chains — the streaming source adds ONE bounded tx per page, not per
record. See `perf-evidence-workflows-streaming.md` and the captures it
cites (`.measurements/t20-streaming-bands-*.json` — the range is every
captured run, never the friendliest).

## §4 — HITL: humans are rows (T10)

`ctx.wait_signal((Approval, Escalate), timeout_s=…, tool=…, args=…,
reason=…)` — THE TYPED WAIT. The tuple form is the typed wait (PEP 604
unions in value position carry no static payload information — the
return type is the union and narrows with `isinstance`); the
single-payload form is the one-member overload.

**THE RESUME CONTRACT, stated as a feature** (cut #18's disposition):
the body re-executes **FROM THE TOP** on resume — there is NO
determinism requirement on the body; pre-wait side effects are
`ctx.substep`-ledgered and replay cheap. The delivered holds are the
node's ANSWER QUEUE: each attempt's wait sequence consumes them in
epoch order (the per-attempt cursor) — a RETRY replays the answers (the
operator never re-answers); a wait past the queue's end registers a NEW
hold (a NEW epoch — the multi-hold: the same signal name can hold
again, the chained gate works because the waits are sequential).

**THE PAYLOAD RIDES THE ROW** (cut #3's cure): the human's answer IS
the row's payload; `ctx.signal(name)` reads it on resume (the read
before delivery is the typed `SignalUnavailableError` — never a silent
None).

**THE REPLY HANDLE**: every hold's `id` (uuid7, time-ordered) is THE
reference — `HitlClient.resolve(id, decision)` addresses ONE hold; the
`(node, signal, epoch)` identity disambiguates MULTIPLE holds. The
resolve is IDEMPOTENT (an already-resolved hold is the DEFINED no-op)
and AUDITED ("who resolved this" is a ROW, not a log line).

**MAP SIBLINGS HOLD CONCURRENTLY, AS DISTINCT ROWS** (the LedgerWatch
F2 cure — the per-item approval is the machinery's OWN shape): the hold
identity is
`(workflow_id, node_key, signal_name, COALESCE(map_index, -1),
hold_epoch)` — the map index rides the identity, so two items of one
`map_source` holding on the same signal name are TWO hold rows
(`HitlClient.list(run=…)` lists BOTH; each `HoldContext` carries its
`map_index` — the UI renders the item it addresses). Each resolve wakes
exactly the hold it names: the resume addresses the hold's OWN node by
the hold id stamped into that node's metadata — one hold, one node,
one answer consumed by its own sibling. The pre-cure shape (one shared
identity per node key) collided the siblings — the second sibling
wedged `running` forever in-process, or its hold INSERT died of the
unique arbiter into the ladder and its retry STOLE the first sibling's
delivered answer. A per-item review workflow (one hold per item, all
concurrent) is spelled directly on the map — no per-item run keys, no
side table.

**THE HOLD ROW'S STATUS VOCABULARY** (the LedgerWatch F8 cure — the
four spellings the ROWS carry, in one table; the guides previously said
"pending" for the wait and "abandoned" for the expiry while the rows
say `held`):

| `status` | how a hold gets there | what the wait site does on resume |
| --- | --- | --- |
| `held` | the wait site registered it (the live hold) | the wait site re-reads it (the idempotent re-hold) |
| `delivered` | a resolve/deliver won the CAS | the answer queue replays its payload |
| `abandoned` | the expiry sweep fired past `expires_at` | the wait returns the `Expired` member (the fail-close) |
| `cancelled` | the run's cancel cascade swept it | the flow is terminal — the body does not resume |

All four live on `wf_signals.status` (partial-unique over the
UNRESOLVED holds only — `resolved_at IS NULL`); `resolved_at` is set
by every non-`held` transition.

**THE CONTEXT CONTRACT** (round-8): the hold carries the payload-model
schema (what a UI renders from), the author-supplied
reason/tool/args, the provenance (run id, node key, gate name, epoch,
created_at, the deadline). **THE REDACT LAW EXTENDS — BEFORE PERSIST**:
the context passes the chain-then-hook redact
pipeline (the vetted masks + the hold surface's token-head pass + the
workflow's own `redact=` hook, wired from the registered definition) IN
THE SAME TRANSACTION as the hold's insert — the row never carries a
canary, so the default `HitlClient` (no hook handed, none wired) cannot
leak it at list/get; the read-side chain pass is the belt over the
persist-time law.

**THE KNOB, NEVER THE TRUTH** (round-8): a hold's appearance is
NOTIFIABLE (`pg_notify` on the `taskq_wf_holds` channel — the estate's
notify discipline); the notification carries THE POINTER (hold id + run
id + event) — a lost knock costs latency, never correctness: the
consumer converges by polling `HitlClient.list(run=…)`.

**THE BROADCAST, TYPED (T26)**: the knob's legs are now three
TRANSACTIONAL broadcast channels — `taskq_wf_hold_created` (the hold
CREATED, inside the insert's own tx), `taskq_wf_hold_resolved` (inside
the CAS-winning resolve/deliver tx, carrying `verdict_kind`: the
payload model the typed door validated against), and
`taskq_wf_hold_expired` (the sweep abandoned it). The channels are
GLOBAL and the payload carries the SCHEMA (`pg_notify` is
per-database; the schema-per-module estate shares one database across
many schemas — the listener filters by payload, never by channel
arithmetic; that filter IS the isolation between the estate's
schemas). A rolled-back hold-create is SILENT by construction: PG
delivers a NOTIFY only when its transaction commits — and a
rolled-back RESOLVE is the same silence (the CAS + the audit + the
broadcast ride one tx). The typed consumer is
`taskq.workflows.HitlListener` — DIRECTLY ASYNC-ITERABLE, the events a
CLOSED union — and it BACKFILLS FROM THE ROWS at start (LISTEN first,
then the open-hold snapshot): a hold created before the listener
existed is still delivered, and its own late NOTIFY is deduped by
`hold_id` — the missed-event window is ZERO by construction. EVERY
(re)backfill ends with the `Backfilled(open_hold_ids=…)` reconcile
snapshot (the union's fourth member): a hold resolved or expired while
the listener was DOWN never announces its own death, so the consumer
drops any open card the snapshot disowns — and any stale event replayed
from before the outage dies the same death. Every subscriber gets its
own queue and its own end sentinel (the fan-out: N watching users, each
seeing every event — never a partition). The admin's `/sse/{topic}`
surface gained the `holds` topic: the same typed events (backfilled at
subscribe) streamed to the browser over the same session re-check
every other topic keeps. The resolve door is unchanged.

**THE LISTENER'S OPERATIONAL LANDMINES** (each one real; the listener's
docstring carries the same four):

1. **PgBouncer**: transaction-pooling (the default `pool_mode =
   transaction`) SILENTLY breaks LISTEN — a LISTEN is session-scoped
   and a transaction pooler hands the session away after every tx, so
   notifications stop arriving with no error anywhere. The listener
   OWNS its dedicated connection for the stream's life precisely for
   this: it must be a DIRECT connection (bypass the pooler, or run a
   session-pooling port). This is the #1 real-world NOTIFY failure.
2. **The capacity tax**: that dedicated connection is ONE POOL SLOT
   FOR THE STREAM'S LIFE. Size the pool for the listeners you run; an
   SSE face holding a listener per browser tab spends one slot per tab.
3. **Fork safety**: the dedicated connection is an asyncio transport
   bound to the loop and process that opened it (asyncpg is documented
   fork-unsafe — `_forkguard`). An app that forks after the listener
   starts (uvicorn/gunicorn multi-worker, `--preload`) must start the
   listener IN EACH WORKER after the fork — never inherit a parent's
   live listener.
4. **Expiry precision is AT-LEAST**: the deadline fires on the DB clock
   at the first leader-sweep pass at-or-after it — worst case
   `timeout_s` + the sweep interval + a leader failover's gap. Never
   promise a precise 120 s: the expiry is a floor; the fail-close is
   the guarantee.

**THE EXPIRY IS A VALUE, NOT AN EXCEPTION (T26's amendment)**: the
wait's outcome is the CLOSED UNION of the declared models joined with
`Expired` — the CHECKER FORCES the fail-close arm. From the REAL
example (`examples/deep_research.py`):

```python no-exec — not executed: fragment, the body's spine — the
REAL file is examples/deep_research.py (the demo's fake corpus + the
loop wiring live there)
outcome = await ctx.wait_signal(
    (ContinueApproval,),
    timeout_s=APPROVAL_TIMEOUT_S,  # the REAL default: 120.0
    reason="the research loop wants to continue past the free passes",
)
match outcome:
    case ContinueApproval() as approval:
        if not approval.approved:
            return Done(carry.finish_with_what_you_have(note=approval.note))
        carry = carry.model_copy(update={"approved": True})
    case Expired():
        # THE FAIL-CLOSE: nobody watching — finish with what you have.
        return Done(carry.finish_with_what_you_have())
```

The typed expiry is a RESULT the body matches: the run SUCCEEDS
carrying the finish-with-what-you-have state — the user not watching
is a RESULT, typed and named, never a hang and never a failure. A body
that ignores the arm reds the checker (the fall-through against the
body's declared return; the bare unwrap against the missing attribute —
the type-probe corpus holds both markers). `SignalTimeoutError` stays
in the vocabulary for the body that WANTS the failure — the escalation
ladder's own use: raise it yourself off the `Expired` member. (The
example honors the loop's shape law — ONE wait per iteration; the
scenario's "three loops" are three research passes inside the first
iteration, because the answer queue's cursor IS the iteration counter.
The law has TEETH now — the W5-loop-wait-shape rule: a loop-kind body whose wait is
CONDITIONAL (some iterations wait, others don't) leaves the iteration
cursor past the answer queue — stranded answers, retries replaying the
wrong slot — and the wait site REFUSES the mis-aligned state with the
named `LoopWaitShapeError`. A plain step's cursor is
consumption-ordered; conditional waits are legal there.)

**THE TIMERS**: the deadline is DB-CLOCK compared (the signal sweep's
expiry arm is the ONLY live timer on a held row); the expired hold →
the DEFINED `abandoned` state (the resume's wait site returns the
union's `Expired` member — the typed face, never a silent orphan);
`timeout=None` must be EXPLICIT (the W1 validate warning).

**THE TIMEOUT FACE IS THE UNION'S SECOND ARM** (T26's amendment): after the sweep marks the hold `abandoned`, the
resume's wait site RETURNS the `Expired` member — the fail-close arm
the CHECKER forces — and NEVER mints an automatic new epoch (hold →
expire → re-hold → ∞ is the convicted dragon, kept red by the attack
probe). A body that WANTS the failure raises `SignalTimeoutError`
ITSELF off the member (the escalation ladder's own use — the machinery
never raises): a step ladders and terminal-fails (the `fail` policy's
shape); a loop's failure-class rules route it as the BODY failure it
is. A DELIBERATE re-wait — the body MATCHED the expiry member and
waits again within the same attempt — is a NEW body decision: it
registers a NEW hold with a NEW epoch. The other timer policies
(`resume_with_default` / the escalation arm on the TIMER) are recorded
LATER for v1 (the don't-pay law): the body-level escape — match the
`Expired` member, return the default / enqueue the escalation
yourself — is the sanctioned composition until then, and it is exactly
what the value-based face enables.

**THE DELIVER BOUNDARY IS REAL**: a resolve
validates the payload against the hold's DECLARED models — BY SHAPE,
never by declaration order — BEFORE the CAS: a payload that fits NO
model, or fits MORE than one without the gate's `discriminator=`, is
the typed `refused` (the hold SURVIVES, nothing is consumed, the
refusal is AUDITED and the operator sees WHY). The union wait narrows
to the model the payload's shape fits — a lenient first member cannot
swallow a strict second member's delivery.

**THE AUDIT IS EXACTLY-ONCE**: the resolve's
audit row + the knock ride the CAS-WINNING transaction — two concurrent
resolves → exactly ONE `hitl.resolve` audit row, ONE knock, ONE
resolution; the loser writes nothing.

**THE CANCEL CASCADE** (P3 rule 4): `FlowRunner.cancel_workflow(run_id,
reason=…)` — ONE transaction (the flip is the linearization point), the
held signals → `cancelled` in the same snapshot, a LATE operator
deliver returns the typed `refused` (no zombie wake, no resume event),
terminal rows untouched, IDEMPOTENT (cancel twice = one cancel), the
audit row rides the same tx.

**RESUME-NOT-RETRY** (cut #5's cure): a hold's resume consumes NO
ladder attempts — the ledger distinguishes `awaited` from `failed`;
the ladder counts `failed` only. The shared-counter variant (2 holds +
max_attempts=3 = terminal failure with zero retries) is red forever.

The hold→resume latency band is measured and pinned in
`perf-evidence-workflows.md` (the band from
`.measurements/t10-hold-resume-band.json`).

## §4b — The durable timer: the clock is a resolver (T28)

"Remind them tomorrow" is a hold whose RESOLVER IS THE CLOCK:
`ctx.wait_until(when, key=…)` — the body sleeps, the run survives, the
wake resumes the body. THE TIMER'S FACE INVERTS THE EXPIRY'S: the
human gate's deadline-pass (`wait_signal`'s `timeout_s`) is the
`Expired` member — the fail-close the checker forces. **The timer's
deadline-pass is THE WAKE** — the sweep resolves the hold
`'held' → 'delivered'` with the clock's own receipt and the body
RESUMES. The timer's point is the continuation, never the fail-close.

```python
import asyncio
import os

import asyncpg
from pydantic import BaseModel

import taskq.migrate
from taskq.workflows import (
    Promise,
    StepContext,
    TimerWake,
    WorkflowApp,
    build,
    run,
    step,
)

app = WorkflowApp()


class FollowUp(BaseModel):
    ticket: str


@app.workflow("reminder_flow")
def reminder_flow() -> Promise[object]:
    return build(step(remind_body, FollowUp(ticket="T-1"), key="remind"))


async def remind_body(ctx: StepContext, params: FollowUp) -> str:
    # THE ONE-DAY SHAPE: `ctx.wait_until(when, key="remind")` with
    # `when = datetime.now(timezone.utc) + timedelta(days=1)` — here a
    # 1-second float so the verified run finishes fast. The pre-timer
    # side effect is SUBSTEP-ledgered: the resume re-executes the body
    # from the top and the substep replays (never re-runs).
    wake = await ctx.wait_until(1.0, key="remind")
    return (
        f"ticket={params.ticket} wake={wake.key} by={wake.fired_by} late={wake.lateness_s >= 0.0}"
    )


async def main() -> None:
    dsn = os.environ["TASKQ_PG_DSN"]
    schema = os.environ.get("TASKQ_SCHEMA_NAME", "taskq")
    await taskq.migrate.apply_pending_locked(dsn, schema=schema, phase="pre")
    await taskq.migrate.apply_pending_locked(dsn, schema=schema, phase="post")
    pool = await asyncpg.create_pool(dsn)
    outcome = await run(app.get("reminder_flow"), pool, schema)
    print(f"claim={outcome.claim.kind} outcome={outcome.outcome} result={outcome.result}")


asyncio.run(main())
```

Verified output (this guide's capture, on a fresh schema):

```
claim=created outcome=terminal result=ticket=T-1 wake=remind by=timer late=True
```

The machinery, and the laws each piece carries:

- **THE ROW IS THE TRUTH (the hold's law, unmodified)**: the timer IS
  a `wf_signals` hold row under the reserved `__timer__` signal
  prefix. The held representation releases the worker's SLOT (no
  lease, no claim, no attempt burned while sleeping); a LOOP node's
  budget pauses while the timer stands — the wall is blind while the
  body waits on the clock, exactly as while it waits on a human.
- **THE SWEEP'S TIMER ARM** (the expiry arm's inverted twin, wired
  beside it in the leader's signal sweep and in the dev-loop `tick`):
  every due timer hold resolves `'delivered'` — never `'abandoned'` —
  with the WAKE as the payload (`fired_by='timer'`, the timer's
  `key`, the `scheduled` and `fired_at` instants). The `'held' →
  'delivered'` CAS is the fence: a twice-fired timer is
  unrepresentable, and a cancelled flow's timer never wakes.
- **THE DURABILITY IS THE DESIGN**: a worker DEATH mid-wait changes
  nothing — the hold row's expiry arms the reclaim, the successor's
  sweep fires the wake, the body resumes from the top (the
  re-execution doctrine: pre-timer side effects are
  `ctx.substep`-ledgered and replay). The pin drops the arming driver
  and lets a fresh runner's sweep complete the flow.
- **THE AT-LEAST FACE** (the expiry's own precision law, restated):
  the fire lands at the FIRST sweep pass at-or-after the deadline —
  never early, never at a precise wall time. `TimerWake.lateness_s`
  (the `fired_at` − `scheduled` delta) is that honesty, on every
  wake. Pass a `float` (seconds; the DB clock owns the base) or a
  `datetime` (the absolute instant; naive means UTC — the DB never
  re-bases it).
- **THE TIMER IS NOT A HUMAN HOLD**: `HitlClient.list()` never
  enumerates it (the reserved prefix is the split's mark — the
  display surfaces show it AS a timer), and the resolve boundary
  REFUSES any hand-delivered payload — a well-formed forgery of the
  wake's schema included. The clock is the only resolver this hold
  takes.
- **THE INTERPLAY**: the cancel cascade kills the timer with the flow
  (it is an ordinary hold row — the same one-tx cascade); `Expired`
  and `TimerWake` are orthogonal in one flow (a human gate's
  fail-close beside a clock's wake fan in as two named outcomes); a
  mid-wait REDEPLOY re-enters the NEW body on the wake's resume (the
  versioning contract, §6). In a LOOP the timer shares the answer
  queue's cursor — the ONE-wait-per-iteration shape law (W5) reads a
  conditional timer exactly as a conditional `wait_signal`.

**THE NAIVE-DATETIME LAW** (the silent coercion, stated at the API
face): a naive `datetime` — no `tzinfo` — passed to `wait_until` is
interpreted as UTC. The coercion is silent (`when.replace(tzinfo=UTC)`):
no error, no local-zone reading — `17:00` naive means `17:00 UTC`, not
your server's 17:00. The DB never re-bases the instant: the hold row's
`expires_at` is exactly the coerced value, the wake fires at that UTC
instant, and the wake's `scheduled` stamp reads it back unchanged. Pass
a tz-aware `datetime` (`datetime.now(timezone.utc) + timedelta(...)`)
to mean what you said; a naive one is accepted for continuity and reads
as UTC. (A `float` of seconds keeps the relative face — the DB clock's
own `now` is its base.)

## §5 — Progress: the nodes report (T21)

A workflow body reports its own progress: `await ctx.progress(75, "page
3/4", {"page": 3})` — `pct` (int 0..100 or None), `message` (chars-capped
at 1024), `data` (jsonb, capped at 8 KiB). All optional; call it as often
as you like.

**THE EMISSION IS BEST-EFFORT, AND THAT IS THE DESIGN** (decision e — the
law: observability degrades FIRST, never correctness). The call updates
an in-memory buffer latest-wins and arms a cadence flush (~20 writes/s at
the 50 ms cadence — a 1000-emissions/s body coalesces to the same write
rate); it NEVER awaits the network and NEVER touches the finalize path.
The coalescing is HONEST: the node's `occurrences` counter counts every
emission that coalesced into the row, so the record shows both the
delivered state and the emission rate. A flush that fails is counted and
warned once (`progress-flush-lost`); a node whose every flush fails still
terminalizes normally. A lost emission costs freshness; a blocked node
costs correctness.

**THE TWO CHANNELS** (decision d — both bounded by construction):

| channel | table | shape | bound |
| --- | --- | --- | --- |
| the STATE channel | `wf_node_progress` | one row per `(node_id, channel)`, upserted latest-wins + the occurrence counter + the dropped counter | nodes × channels — CONSTANT under any emission rate |
| the STREAM channel | `wf_node_stream` | append rows in a per-node ring, drop-oldest with the dropped count ON THE RECORD | the ring bound (64/node; the retention sweep's prune arm is the backstop) |

The state channel answers "where is this node NOW" (a gauge). The stream
channel answers "what did it report along the way" (a replay). **The
state channel keeps NO history**: "what did the body report at 14:32" is
answerable only where the author streamed it (the stream) — the honest
boundary, stated in the progress guide.

**THE DECLARED SCHEMA** (the TypedGate-door pattern): `step(body, ...,
progress_schema=Page)` declares the shape of the node's `data`
emissions — a wrong-shaped emission is refused with
`ProgressRefusedError` (raised INTO the body: an authoring error is the
body's problem). The declaration is what makes a separate UI render the
emission — the context-contract law applied to progress. On a MAP SOURCE
the declaration reaches the children (`<source>.item`).

**AUTO-PROGRESS IS A PROJECTION, NOT A SECOND STREAM** (decision b): the
engine's node-start/terminal events project into the SAME stream with
`class='auto'` (the body's emissions are `class='user'`); the kind
vocabulary is CLOSED (`progress`, `wf.node.started`, `wf.node.terminal` —
validated at emit AND by the storage CHECK), and ONE seq generator backs
the table. ONE stream, ONE cursor serves both classes — a second
seq-cursor space would re-create the fleet's SSE-vocabulary
fragmentation. The projection is additive best-effort writes at the
claim/finalize seams: the finalize's own transactions are untouched (the
zero-finalize-changes probe is a shipped pin).

**THE PROGRESS-LIE FENCE** (decision b's display law): the node's STATUS
derives from the LEDGER; the progress renders INSIDE the terminal state,
never over it. A body that reports 99% "almost done" and then FAILS
shows **failed with pct=99 inside** — never 99%-running. A crash window
(the worker died mid-emission) leaves the display STALE — freshness
only — and the terminal heals it. The progress row is ADVISORY, always.

**THE AGGREGATION IS DERIVED AT READ** (decision c): a map's progress is
its children's grouped read (`"120/200 · 0 running · avg pct 62"` — one
query, LEFT JOINed to the state channel); a fan-in's is the edge ledger.
The user's aggregate of intermediate results WITHOUT a join node: declare
`map_source(source, item, aggregate=fn)` — a PURE, read-side fn evaluated
AT READ TIME over the children's decoded result rows (mid-flight,
unblocked, writing nothing). **The join node is for DATAFLOW; progress
aggregation is OBSERVABILITY** — a join whose only reader is the display
is the W2 validate warning (the join-for-progress anti-pattern: the DAG
would block on a display question).

**THE FACES** (decision f — every face a view over the same rows):

- the SSE stream: `GET /api/flow/{flow_id}/progress/stream` on the admin
  router — a `display` frame first (ledger + state), `progress` frames in
  seq order (`id:` the seq — the browser EventSource reconnect
  contract), and — when the ring pruned past a reconnecting cursor — a
  NAMED `resync` frame (the partial mode + the state payload): the
  display converges, never a silent empty-success.
- the run display:
  `taskq.workflows._progress_read.run_display(pool, wsql, flow_id)` —
  the ledger-derived states with the progress inside (the explorer's
  data contract); `keyed_display(display)` renders it name-first. The
  `wsql` is the runner's own statement bag (`runner.wsql` — the
  `FlowRunner` builds it from the schema); the display plumbing is an
  ENGINE INTERNAL by the package's own surface law, so the import is
  from its submodule — the documented path
  (`from taskq.workflows._progress_read import keyed_display,
  run_display`), never from the root namespace. See
  `docs/guides/progress.md`'s executable fence.
- the gauge: workflow-dimensioned ONLY (the `_other_` collapse) —
  **per-child progress is NEVER a metric label series**; it lives in the
  rows (the map line / the display), read on demand.

**THE VOCABULARY SHIMS** (the migration win, documented): the fleet's two
live SSE vocabularies are MAPPED, not converted — each is a pure function
over the one stream's rows:

| fleet vocabulary A | the stream | fleet vocabulary B |
| --- | --- | --- |
| `{"type": "progress", "job": <node>, "pct": <pct>, "note": <message>}` | `class='user'`, `kind='progress'` | `{"event": "update", "id": <node>, "percent": <pct>, "detail": <message>}` |
| — | `class='auto'`, `kind='wf.node.started'` | `{"event": "lifecycle", "id": <node>, "phase": "started"}` |
| — | `class='auto'`, `kind='wf.node.terminal'` | `{"event": "lifecycle", "id": <node>, "phase": "done"}` |

A consumer of either old vocabulary is served by the ONE stream through
its shim unchanged — the consolidation is the migration win (the third
vocabulary cannot sprout: the kind set is closed at the emit path and by
the storage CHECK).
---

## §6 — The in-flight versioning contract (T29)

**THE DECISION — the registry-is-truth contract**: a REDEPLOYED
definition is adopted by the in-flight runs at their NEXT NODE CLAIM.
The compile's STRUCTURE is the create-time snapshot (the nodes, the
edges, the wiring — a run's graph never morphs mid-flight); every BODY
resolves from the CURRENT registry at the claim seam (the resolution
is per-claim, never per-run — the registry answers first, the compiled
node's own body is the fallback when the registry lacks the key). A
redeploy keeps the same node KEYS, so the registry hit is the norm:
the new code runs the old run's remaining nodes.

TODAY'S HONEST PREDECESSOR: before this section existed, the same
adoption happened SILENTLY — a mid-flow deploy changed in-flight
behavior with no contract text and no taught read. The contract names
the behavior; the STAMPS record it.

**THE STAMPS ARE THE OPERATOR'S READ** (§22.1, landed with T03): every
node claim writes `jobs.code_version` — the body's canonical content
hash (the tors content hash over the named payload: module, qualname,
source, and the decorator's explicit pin when given). WHICH code ran
each row is a ROW fact, per node, per attempt:

```sql
SELECT step_key, attempt, code_version
FROM taskq.jobs
WHERE (metadata->>'flow_id')::uuid = $1
ORDER BY id;
```

A run whose rows carry TWO different hashes for one node = the deploy
drift happened mid-run, and the rows say which side of it each attempt
ran. The hash is a RECORD, never a gate: a body the hash cannot read
(a builtin, a partial) records as NULL — the loss is logged, the claim
never fails on its own audit.

**THE REMEDIATION** (the operator's tool when the adoption is NOT what
a run should continue with): the **cancel-then-restart path** —
`FlowRunner.cancel_workflow(run_id)` (or the admin's cancel door),
then re-run on the current code under a NEW run key (the run-key
arbiter's own law: the re-run is a NEW key, never a replay of the
cancelled run's). There is no per-run code pin in v1: a run mid-flight
cannot be HELD on old code — the adoption is the contract, the
cancel-restart is the escape.

**THE EDGE CASES, NAMED** (each re-enters the NEW body through the
same claim-time resolution — the pin covers the second):

- **A run mid-LOOP**: the iteration's body re-resolves at the resume —
  the next iteration (and the resumed one) runs the NEW code.
- **A run mid-WAIT**: the wake (the timer's, T28) or the human
  delivery's resume re-enters the NEW body — a wake is a resume like
  any other, and the claim-time resolution is the only body source.
  The answer queue's replay law holds ACROSS the deploy: the new
  body's wait site consumes the already-delivered answer (the
  operator's re-run never re-sleeps, the operator never re-answers).
- **A cached step** (T25): the cache's address INCLUDES the
  `code_version` — a redeploy is a NEW address, so the new code's
  results land on the new code's cache line and an old line is never
  served to a new body (the T25 design's own law — the stale-truth
  hazard's structural kill).

**THE SNAPSHOT-PER-RUN DESIGN — recorded, not built**: pinning each
run's code AT CREATE (the create stamps the definition snapshot, the
run reads its own snapshot forever) is the LATER design — it re-derives
the compile-time registry discipline and is recorded in
[the spec's open LATER rows](../design/workflows-spec.md) (§8), not
shipped. Until it lands, the registry-is-truth contract above is THE
behavior — rely on the stamps to see it, and on cancel-restart to
escape it.

**THE WEDGE, NAMED** (the versioning wedge — know it before you
rename): because the CURRENT registry is the only body source, a
redeploy that RENAMES or REMOVES a node wedges every in-flight run
sitting at it: the run's node row still exists (the create-time
structure snapshot), but the current graph no longer declares the key,
so the node can never claim a body again — the run is a ZOMBIE: never
terminal, nothing fails it, and nothing else in the CLI or the admin
surface said so. The drift is now VISIBLE: `taskq flows status <run_id>
--app yourapp.workflows:app` compares the run's node inventory against
the current registry's compiled graph and names the stranded nodes (a
node the current registry does not declare), the zombie law, and the
remedy (cancel + re-run). Related, and equally entailed by the
contract: a mid-map redeploy provably MIXES v1+v2 results in one run —
the fan-out's children claim the body one at a time, and the code
changed between claims (the registry-is-truth contract IS the mixing;
this is not a defect to fix but a fact to see). The per-node stamps
(`jobs.code_version`) are the read that says which side of the drift
each attempt ran; the map's summary row can carry mixed versions by
design. Escape hatches, both: the cancel-then-restart path above, or
re-deploy the old definition to let the run finish before re-deploying
the new one.

---

## The context contract (what a body asserts on)

The body's `ctx` is the observability primitive: every field the contract names is on the LIVE surface (a body asserting on ctx is a pin, not a mock):

| Field | What it is |
|-------|-----------|
| `ctx.flow_id` | the run's id (the address every surface takes) |
| `ctx.job_id` | this node attempt's job row |
| `ctx.node_key` | the node's key — the map's children run under `<source>.item` (the per-item ledger identity) |
| `ctx.attempt` | the attempt ordinal (the ladder's count; holds do not advance it) |
| `ctx.input` | the FLOW input (`create_flow(input=...)`) |
| `ctx.flow_name` | the workflow's registered name |
| `ctx.queue` | the queue this node rides (the wiring's placement) |
| `ctx.claimed_at` | the attempt's claim timestamp |
| `ctx.map_index` | the map item's index (None off a map) |
| `ctx.budget_remaining_ms` | the loop's budget wall's remaining read (loop runs; None off a loop) |
| `ctx.hold_epoch` | the last consumed hold's epoch (a resumed body's answer identity) |

**The two doors' contracts differ (stated, not hidden):** the STEP door coerces — a body handing a pydantic model to `step(...)` has it re-validated at the boundary (the dict round-trips as the model). The EMIT door refuses — `ctx.progress(...)`'s payloads are dicts/bytes (the wire's own vocabulary); a pydantic model handed to emit is the LOUD `ProgressRefusedError` (an authoring error is the body's problem). Pass `model.model_dump()` at emit. (The ergonomics cure — the emit door accepting models — is owed to the T20 lane; until then the refusal is the contract.)

## The observability contract (the workflow spans)

The vanilla lifecycle emits spans at every messaging point (`enqueue
{actor}`, `process {actor}`, `attempt.{N}`, `dispatch`, `cron fire` —
see [the observability guide's span hierarchy](observability.md#2-traces-span-hierarchy)).
A workflow's rows never ride that path — the fleet intercept bypasses
`consume_one_job` and the dev drive never enters it — so the workflows
package emits its own spans at the wf lifecycle's points, with the SAME
machinery and the SAME laws (no new otel machinery, no new metrics):

- **The RUN span** — `run {workflow}` (INTERNAL): one per
  [`taskq.workflows.run`](#1-the-flow-api-the-concept-t09) call. It is
  a **root** (`new_root=True`, the producer-link pattern): linked, never
  parented, to the ambient trace — a caller's own span is not held open
  for the run's lifetime, the link is the correlation. Created at the
  flow's first claim (the run-key claim) and ended at the drive's stop.
  Child flows (T28) carry no run span of their own — their steps ride
  the tree below their parent's.
- **The STEP span** — `step {step_key}` (INTERNAL): ONE attempt of one
  node through the claim tail — the one shared tail the dev drive and
  the worker-hosted fleet door both land in, so both faces emit it. When
  the run drives in-process (`execute=True`), the step span is the run
  span's **child** (otel's in-process context propagation). On a fleet
  worker there is no run span in that process, so the step span is a
  **per-process root**: the vanilla's cross-process law is
  producer-stamped ids on the job row plus a consumer-side link, and wf
  rows have no producer span to stamp otel ids from (the row's
  `trace_id` column carries the wf per-record lineage id — a different
  concept). The cross-process correlation is therefore the
  **`taskq.flow_id` attribute**; no traceparent crosses processes.

**The attribute contract** (the vanilla `taskq.*` namespace):

| span | attribute | value |
| --- | --- | --- |
| run | `taskq.workflow` | the registered workflow name |
| run | `taskq.flow_id` | the run's root-row id (set the moment the claim returns) |
| run | `taskq.queue` | the root row's queue (a constant by construction — the steps carry their own) |
| run | `taskq.run_key` | only when the caller declared a `key=` — UNREDACTED caller-controlled text (the observability guide's own caveat for `taskq.identity_key`: attribute cardinality is free but the value rides whole; declare keys accordingly — a slot timestamp, never an email + token) |
| run | `taskq.priority` | only when the caller declared a `priority=` |
| run | `taskq.outcome` | the drive's label: `terminal` / `held` / `max_ticks` |
| step | `taskq.flow_id`, `taskq.workflow`, `taskq.step` | the flow link, the name, the step key |
| step | `taskq.attempt` | the attempt ordinal |
| step | `taskq.queue` | the node's queue, when the node's decl carries one (the fork-spawned map children have no decl) |
| step | `taskq.map_index` | the map item's index, when the row has one |
| step | `taskq.outcome` | the claim tail's own label: `succeeded` / `skipped` / `loop` / `held` / `deferred` / `fenced` / `laddered` / `cancelled`, and `crashed` for the machinery boundary's re-raise |

**Status and events** follow the vanilla consumer span's laws. Status:
OK on the succeeded class, ERROR on the failed class, the mid-states
(held, deferred, fenced, cancelled) UNSET. The lifecycle events reuse
the vanilla grammar (`lifecycle.running`, then
`lifecycle.succeeded`/`lifecycle.failed`/`lifecycle.cancelled` with
`from_state`/`to_state`). The fan-out's shape is EVENTS on the spans,
never separate spans — `wf.forked` (the map source's children count),
`wf.join_fired` (the join keys that fired in the finalizing tx),
`wf.held` (the hold id + the signals), `wf.deferred` (the signal + the
delay), and `wf.laddered` (the error class + whether the ladder
re-pended) beside the SCRUBBED `exception` event.

**The redaction law** holds unchanged: a failing step's error is
rendered through the obs layer's scrub pass once — the Postgres DETAIL
row values and the URI credentials never reach the exporter. **The
no-op law** holds unchanged: with telemetry off (`TASKQ_OTEL_ENABLED=false`)
the attribute documents are never built, no span is created, and the run
is byte-identical (a run under spans and a run without them produce the
same rows). **Metrics**: none are added — the vanilla's one wf-relevant
instrument is the maintenance leader's fleet gauge
(`taskq.wf_progress_nodes_total`); no per-run wf metric exists to
mirror, and the per-node truth lives in the rows and the run page.

## Visualizing workflows: the admin's run explorer

The admin UI's **Workflows** tab is the run explorer: the runs list (the inventory question — newest first, capped at 200), and the run page (`/taskq/workflows/{run_id}`):

- **The graph** — the run's Mermaid, emitted FROM THE ROWS (the live graph is a row fact; the admin never imports the workflow's module): the collapsed map renders as ONE hexagon with a `done/total` counter — zero child boxes (the counter reads the children's TRUE parent — the map source the fork parented them at — and the run header carries the SUM across sources); the render does not scale with the fan-out (a 10k map renders one hexagon, never 10k lines). The child detail lives in the node panel. Rendered ONCE by the vendored mermaid (no bundler, no React, no CDN — the air-gapped-ops argument); every later update is a classList patch on `data-node-key` — zero re-renders (the node keys and statuses are sanitized/escaped against the legend's closed vocabulary — a poisoned row key cannot blind the live updates). The state colors: green succeeded, blue running, red failed, amber HELD (a human is waiting).
- **The node panel** — click a node (or Tab to it and press Enter): the status header, the attempts + the retry kind, the trace id, the captured error (errors-only by default; the panel's read is DISPLAY-CAPPED with the dropped-count named — the full detail stays in the rows), one upstream hop (the causal chain via parent_id), and the attempt ledger.
- **The deferral line** — a step sitting on a body-raised [`Snooze`/`RetryAfter`](#the-control-flow-signals-the-same-typed-signals-the-vanilla-bodies-raise) renders the line beside its status chip: the signal that deferred it, the NEXT FIRE TIME, and the node's snooze count — the operator's "why is this step sitting here" answer without leaving the page. The line is the ROW's truth (the deferral stamp's own discriminator): it appears when the node arrives at the snoozed state and clears when the step re-claims or a later ladder repend lands (never a stale decoration), in the server snapshot and on the live SSE frames alike.
- **The runs list's deferral badge** — the same state at the RUN level: a run with a node sitting on a live deferral renders the badge on its runs-list row (the sitting nodes' count, the earliest next fire, the deferral depth) — a pathologically deferring run is visible from the INVENTORY, never only after opening the run. A run with no sitting deferral renders no badge.
- **The holds & decisions** — the held row's waiting-on state: the gate, the node, the epoch, the deadline, the author's reason/tool/args, the DECLARED payload schema, and a prefilled valid example. The Resolve form delivers through the typed door (the wrong payload answers the named pydantic error inline; the hold survives — and the REFUSAL ITSELF is audited: the trail records the refused attempt with its reason, so probing the door is as visible as using it). Resolved holds render as decisions (the rows-alone audit).
- **The audit trail** — every operator action on the run (resolve/deliver/cancel) is a ROW here: the principal, the action, the reason. This includes the REFUSED actions — a shape-refused resolve or deliver writes its `refused` row (the refusal is the action). Reads are never audited.
- **The live transport** — SSE (`/taskq/api/runs/{id}/stream`): every frame is a revisioned FULL snapshot; the seq-cursor drops stale frames; `Last-Event-ID` (EventSource supplies it for free on reconnect) continues the cursor and the next frame carries the whole state — **the reconnect never loses state**. The stream re-checks the admin session every poll — a session revoked MID-STREAM ends the stream at the next tick (the same law `/sse/{topic}` keeps; a verifier error or a hung verifier fails closed). An UNKNOWN run id is a `404` at subscribe (the same subscribe-time refusal `/sse/{topic}` gives an unknown topic) — a run that exists but has no node rows yet still streams. The page sets `suppress_refresh=True` (the meta refresh would destroy the live SVG).
- **The no-JS story, stated honestly**: the no-JS surface is the SERVER-RENDERED INITIAL SNAPSHOT (the state at page load, from the same grouped query the boot JSON carries). No interactive no-JS mode is invented; without JS the page is a snapshot, and says so where the live badge would be.

## §7 — the diagnosis runbook

A workflow is stuck. The order of questions:

1. **What is the run's state?** `taskq flows status <run_id>` — the §17.5 derivation over the node rows; every stuck node names its waiting-on state + its remedy. The root row's own status is a CACHE (the rows are the truth — the page says so beside the derived status).
2. **Is it waiting on a human?** `taskq flows holds <run_id>` — the pending holds with their deadlines. A hold with NO deadline is the W1 warning's subject (a workflow that waits forever on a human is a support ticket — the deadline is `timeout_s=`, explicit). The operator's reply: `taskq flows resolve <hold_id> '<json>' --app myapp.workflows:app` (the payload validates through the hold's declared models before anything moves; a wrong shape is refused and the hold survives).
3. **Is a node failed?** The status names it + the ladder's headroom (`attempt` vs `max_attempts`). The manual resume: `taskq flows retry <run_id> <node>` — the ledger is KEPT (the spent attempts stay recorded), the ceiling raises past the spent attempt, the blocked closure re-opens.
4. **Is it a join that can never fire?** The status renders the join-wait rows with the deps counter. A join blocked `failed_parent` re-opens when the failed parent's retry lands; a join blocked `orphan_parent`/`flow_dead` is the fence's record — see the failure-propagation section.
5. **Still stuck?** The admin's run page renders the SAME derivation + the node panel's attempt ledger + the audit trail. The rows-alone law: if the CLI and the page disagree, the ROWS decide — file it as a defect with both outputs.
6. **Is the work slow or is the holder dead? (the stuck-vs-slow triage.)** A body that hangs inside its own code (a long wait, a dead dependency) is never a stuck finding — its row is `running`, which no why-stuck arm names. `flows status` gives running rows their own face: each running row older than a minute prints its running AGE (since `started_at`) and the lease check. The lease check is the verdict:
   - `taskq job show <job_id>` on a `running` row prints the lease face (`locked_by_worker`, `last_heartbeat_at`, `lock_expires_at`). A FRESH lease is work in progress — slow is slow. A LAPSED lease is a dead holder — the row says so (`lease: LAPSED ... the holder is presumed DEAD`).
   - The reclaim sweep repends a lapsed row (or crashes it past the attempt budget) within `TASKQ_SWEEP_INTERVAL` (default 30 s) — **but only while a leader is up.** A lapsed lease with no live worker stays `running` until one returns: if the whole fleet died, the rows wait, and the FIRST 3am fix is to bring a worker back. Confirm the death from the record: `taskq job events <job_id>` carries one `state_change` row with `reason: lock_expired` and the dead worker's id per reclaim.
   - The timers' face: a node waiting on a typed signal (a hold) is NOT running — `taskq flows holds <run_id>` names the hold with its DEADLINE (the wake time: when the timeout fires, the body's `Expired` arm runs). A hold with no deadline is the W1 warning's subject (step 2).

## §7.1 — the operator's rows: the `wf_*` tables and the closed vocabularies

Every workflow fact is a row in the same database; the CLI and the explorer are conveniences over these tables, and at 3am the raw query is always reachable. The tables (all in your schema, default `taskq`):

| Table | One row per | The columns that matter |
|---|---|---|
| `jobs` (the workflow rows) | a run's root (`step_key='__flow__'`), every node, every map child | `step_key` (the positional node key — never changes across holds/retries/reclaims), `deps_pending` (the join counter — a CACHE; the edge ledger is truth), `map_index` (the fan-out slot), `parent_id` (the graph's parent pointer — the fork's source node), `code_version` (the per-attempt code hash), `metadata` (`flow_id`, `workflow`, `blocking_reason`, `failed_parent`, `wf_cache_hit`), plus the vanilla columns (`status`, `attempt`, `max_attempts`, the lease columns, the error columns) |
| `wf_edge` | dependency edge (`child_id`, `parent_id`, `failure_policy`) | the join counter's ONLY truth source; the failure policy the edge was wired with (`collect`/`maybe`/`fail_closed`) |
| `wf_join_fire` | exactly-once join fire (`join_job_id` UNIQUE) | the fire guard's record: at most one fire per joined node, ever |
| `wf_outbox` | a fired join's consumer delivery | `delivered` false = the drain still owes the consumer rows |
| `wf_step_ledger` | one attempt of one step (`flow_id`, `step_key`, `map_index`, `attempt`) | the attempt record: `status` (the attempt's outcome), `result`, `error_class`, `error_message`, `capture` (the failure IO-capture, per the workflow's policy) |
| `wf_signals` | one hold/delivery (`workflow_id`, `node_key`, `signal_name`, `hold_epoch`) | `status='held'` = a pending human wait; `expires_at` = the deadline; `payload_schema` = the declared payload shape |
| `wf_node_progress` / `wf_node_stream` | per-node progress/state-channel rows | the T21 progress faces |
| `wf_step_cache` | one cached step result (`content_address`) | `run_id` (the producing run), `result`, `expires_at` (the TTL) |

**The closed vocabularies** (the values the surfaces render, and what each means):

- A node row's `status` is the vanilla jobs vocabulary: `pending`, `scheduled`, `running`, `succeeded`, `failed`, `cancelled`, `crashed`, `abandoned`. Two derived pending SHAPES exist — join-wait (`pending` + `deps_pending > 0`) and held (`pending` + the unresolved `wf_signals` row). `crashed`/`abandoned` are TERMINALS (zero outbound transitions; the derivation folds them into the failed class).
- The run status is DERIVED from the node rows (§17.5): `running` (any node live or a cancel in flight) → `failed` (any unabsorbed failure) → `blocked` (any hold, join-wait, or blocked row) → `complete` → `cancelled` → `pending`. The root row's own `status` is a CACHE of this derivation.
- `metadata.blocking_reason` (why a pending row cannot be claimed): `join` — a HEALTHY join-wait (fires when its parents finalize); `failed_parent` — a fail_closed parent finalized as FAILURE (the parent's id is stamped beside it in `metadata.failed_parent`); `orphan_parent` — the rederive arm found join-wait rows with no edges (the belt-and-suspenders stamp); `flow_dead` — the fence's record: the flow died and the join can never fire; `body_unavailable` — a fired join whose body never resolved in any worker's process (the delivery CONTINUED; the warning is the record).

## §7.2 — the audit recipe: "what did this run actually do?"

Reconstructing a run's story from the surfaces (no app source, no logs):

1. **The shape:** `taskq flows status <run_id>` — the nodes, the derived status, every stuck row's why + remedy.
2. **The attempts:** each node's `attempt` vs `max_attempts` is the ladder; the LEDGER is the per-attempt record. The CLI's node read (and the explorer's node panel) renders the attempt ledger per node; in SQL: `SELECT step_key, attempt, status, error_class, created_at FROM taskq.wf_step_ledger WHERE flow_id = '<run_id>' ORDER BY created_at` — every attempt's outcome, in order, with the class of every death.
3. **The vanilla events:** `taskq job events <job_id>` — the state changes (`lock_expired` reclaims carry the dead worker's id).
4. **The cache hits:** a node row with `metadata.wf_cache_hit` (rendered by `taskq job show <job_id>` as `cache_hit:` and by the explorer's node panel) NEVER executed — the body ran in the receipt's `run_id`, and `wf_step_cache` holds the delivered result (`SELECT content_address, run_id, created_at FROM taskq.wf_step_cache WHERE content_address = '<address>'`). A bad cached result is evicted by deleting the address's row; the next run re-executes and re-fills it:
   `DELETE FROM taskq.wf_step_cache WHERE content_address = '<address>';`
5. **The graph:** `jobs.parent_id` is the fork's source pointer (every map child points at the source node; node rows point at their wired parents); `wf_edge` is the complete edge list with each edge's failure policy. The parent↔child walk is a two-column query:
   `SELECT child.step_key, child.map_index, parent.step_key AS via FROM taskq.wf_edge e JOIN taskq.jobs child ON child.id = e.child_id JOIN taskq.jobs parent ON parent.id = e.parent_id WHERE e.flow_id = '<run_id>';`
6. **The operator's actions:** the explorer's audit-trail section (and `admin_audit` rows) record every resolve/deliver/cancel — including the REFUSED ones.

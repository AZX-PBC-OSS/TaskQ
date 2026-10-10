# Event-intel pipelines — the streaming fan-out, the typed route, the envelope

An event-intel pipeline ingests an open-ended feed of records — pages,
messages, sensor reads — and runs each record through a route: classify
or screen it, enrich the readable ones, dead-letter the rest, aggregate
the results. The engineering constraints that decide whether it is
production-ready:

1. **The feed is unbounded** — you cannot load it, and a crash mid-page
   must not skip or double-count records.
2. **A fast source must not outpace its workers** without bound.
3. **Every record's outcome is known** — nothing is silently dropped,
   including the failures.
4. **A failed record's siblings never re-run.**

TaskQflow's answer to each: the emit transaction (the cursor
checkpoint), the declared in-flight bound (backpressure), the typed
route (the totality fence), and the per-item retry ladder. The worked
example is `examples/workflows.py` (verified by
`tests/test_wf_demo_legs.py`); this guide walks it.

## The cursor: page by page, crash-safe

A streaming source is a paged generator. Each yield commits ONE page —
the children, the edges, and the cursor checkpoint — as one transaction:

```python no-exec — not executed: verbatim fragment of examples/workflows.py (verified: tests/test_wf_demo_legs.py drives this file; the capture rides the docs lane's report)
async def _screen_source_body(ctx: Any) -> None:
    """The chain source's paged generator: ONE yield = ONE page's emit
    tx (the children + the edges + the cursor checkpoint)."""
    for doc_id in DEMO_DOCS:
        from taskq.workflows.chain import chain_start

        child = chain_start(
            SCREEN_CHAIN, doc_id, map_index=abs(hash(doc_id)) % 32000, trace_id=doc_id
        )
        # THE CURSOR IS THE BODY'S OWN BOOKKEEPING: the emit's tx
        # checkpoints it (the resume continues from the last COMMITTED
        # page — the crash-recovery contract).
        await ctx.emit_batch([child], cursor={"page": 0, "doc": doc_id})
```

The contract, precisely: the cursor value is committed in the SAME
transaction that inserts the page's children. A kill between pages
leaves the cursor at the last COMMITTED page; the resume continues from
there and **re-emits exactly the lost page** — the records between the
last commit and the crash. Nothing before it re-runs (the emitted
children's ledger identity convicts the duplicate), nothing after it is
skipped. The child spec (`chain_start`) stamps each record's
`map_index` (the discriminator: two records' children are two rows, two
ledger keys, two claims — only because it is stamped per record at
emit) and `trace_id` (the one-query lineage: every row of one record's
route carries its trace).

A resume that re-emits a COMMITTED page — or re-pages at a different
width — does not corrupt silently: it dies on a named, typed defect
(the emitted history and the page diverged). You find out at the
resume, loudly, not in the aggregates later.

The runnable fence below shows the whole route end to end — records
emitted, routed by outcome, every terminal a row:

```python
import asyncio
import enum
import os

import asyncpg
from pydantic import BaseModel

import taskq.migrate
from taskq.workflows import (
    DONE,
    Chain,
    FlowRunner,
    Promise,
    Route,
    Step,
    StepContext,
    WorkflowApp,
    build,
    chain_source,
)

_CORPUS: dict[str, str] = {"a": "text a", "b": "", "c": "text c"}


class ScreenOutcome(enum.Enum):
    READABLE = "readable"
    UNREADABLE = "unreadable"


class IndexOutcome(enum.Enum):
    OK = "ok"


class DeadOutcome(enum.Enum):
    FILED = "filed"


async def screen_body(ctx: StepContext, item: str) -> ScreenOutcome:
    return ScreenOutcome.READABLE if _CORPUS.get(item) else ScreenOutcome.UNREADABLE


async def index_body(ctx: StepContext, item: str) -> IndexOutcome:
    return IndexOutcome.OK


async def dead_body(ctx: StepContext, item: str) -> DeadOutcome:
    return DeadOutcome.FILED


CHAIN = Chain(
    name="screen-chain",
    start="screen",
    actor="wf-demo-screen",
    queue="demo-screen",
    steps={
        "screen": Step(
            body=screen_body,
            outcomes=ScreenOutcome,
            route=Route(
                {
                    ScreenOutcome.READABLE: "index",
                    ScreenOutcome.UNREADABLE: "dead_letter",
                }
            ),
        ),
        "index": Step(
            body=index_body,
            outcomes=IndexOutcome,
            route=Route({IndexOutcome.OK: DONE}),
        ),
        "dead_letter": Step(
            body=dead_body,
            outcomes=DeadOutcome,
            route=Route({DeadOutcome.FILED: DONE}),
        ),
    },
)


async def source_body(ctx: StepContext) -> None:
    from taskq.workflows.chain import chain_start

    for record in _CORPUS:
        child = chain_start(CHAIN, record, map_index=abs(hash(record)) % 32000, trace_id=record)
        # THE CURSOR IS THE BODY'S OWN BOOKKEEPING: the emit's tx
        # checkpoints it (the resume continues from the last COMMITTED
        # page — the crash re-emits exactly the lost page).
        await ctx.emit_batch([child], cursor={"page": 0, "record": record})


app = WorkflowApp()


@app.workflow("screen_router")
def screen_router() -> Promise[object]:
    return build(chain_source(CHAIN, source_body, key="screen_source"))


async def main() -> None:
    dsn = os.environ["TASKQ_PG_DSN"]
    schema = os.environ["TASKQ_SCHEMA_NAME"]
    await taskq.migrate.apply_pending_locked(dsn, schema=schema, phase="pre")
    await taskq.migrate.apply_pending_locked(dsn, schema=schema, phase="post")
    pool = await asyncpg.create_pool(dsn)

    runner = FlowRunner(app.get("screen_router"), pool, schema)
    flow_id = (await runner.create_flow()).flow_id
    await runner.drive(flow_id, until="terminal")

    # THE ROUTE'S LEDGER: every record's terminal outcome is a row,
    # traceable by the emit's trace_id — the readable ones indexed, the
    # empty one dead-lettered, NOTHING dropped (the route is TOTAL — an
    # outcome with no arm is the loud RouterNotTotal at compile).
    rows = await pool.fetch(
        f'SELECT trace_id, step_key, status FROM "{schema}".jobs '
        "WHERE trace_id IN ('a', 'b', 'c') AND (metadata->>'flow_id')::uuid = $1 "
        "ORDER BY id",
        flow_id,
    )
    for row in rows:
        print(f"{row['trace_id']}: {row['step_key']} — {row['status']}")


asyncio.run(main())
```

Verified output (this guide's capture, on a fresh schema):

```
a: screen — succeeded
b: screen — succeeded
c: screen — succeeded
a: index — succeeded
b: dead_letter — succeeded
c: index — succeeded
```

Record `b` — the empty text — took the dead-letter arm. Its siblings
were not touched by its outcome.

## The typed route: outcomes, arms, the totality fence

The chain is declared once and instantiated per record. Each step's body
returns its typed OUTCOME (an enum), and the `Route` sends the record to
the next step — or `DONE`:

```python no-exec — not executed: verbatim fragment of examples/workflows.py (verified: tests/test_wf_demo_legs.py)
SCREEN_CHAIN = Chain(
    name="doc-screen-chain",
    start="screen",
    actor="wf-demo-screen",
    queue="demo-screen",
    steps={
        "screen": Step(
            body=_screen_step_body,
            outcomes=ScreenOutcome,
            route=Route(
                {
                    ScreenOutcome.READABLE: "index",
                    ScreenOutcome.UNREADABLE: "dead_letter",
                }
            ),
        ),
        "index": Step(
            body=_index_step_body,
            outcomes=IndexOutcome,
            route=Route({IndexOutcome.OK: DONE}),
        ),
        "dead_letter": Step(
            body=_dead_step_body,
            outcomes=DeadOutcome,
            route=Route({DeadOutcome.FILED: DONE}),
        ),
    },
)
```

The fence that makes this production-grade is the **totality check**: a
body that can return an outcome for which the route declares no arm is
a COMPILE error (`RouterNotTotal`) — the foreign outcome is never
silently dropped, which is the failure mode where a run "succeeds" minus
one record. The route is total or the build refuses.

The chain's shape discipline: at most one child per step's finalize (no
fan-in inside a chain — the route rides the fork-at-finalize machinery);
the record's trace rides every fork; and a map exists for fan-out, next
section.

## Fan-out with a join: the batch face

The chain fans records one at a time. For the batch shape — one step
produces N items, each item runs as its own job, a join collects them —
the verb is `map_source`:

```python no-exec — not executed: verbatim fragment of examples/workflows.py (verified: tests/test_wf_demo_legs.py)
@wf_app.workflow("doc_ingest")
def doc_ingest() -> Promise[object]:
    ingested = step(
        ingest_body,
        IngestBatch(doc_ids=list(DEMO_DOCS)),
        key="ingest",
        actor="wf-demo-enrich",
        queue="demo-enrich",
    )
    enriched = map_source(ingested, enrich_item, queue="demo-enrich", max_attempts=3)
    routed = step(_route_body, enriched, key="route", actor="wf-demo-cpu", queue="demo-cpu")
    ...
    return build(published)
```

The map attaches to the SOURCE node — its finalize forks the children,
cardinality a run-time fact (the source returned 7 items → 7 child
rows). The join key is DERIVED (`ingest.join`); the join collects
`list[R]` and fires **exactly once** (the transactional outbox: at most
one fire per join, ever — two redundant sweep passes move nothing).

## Backpressure: the coalesce order

A source that forks all children up front with no bound is the
unbounded-materialization failure: 10k items become 10k rows and a
queue-global throttle is the only brake. The shipped answer is a
DECLARED per-workflow bound:

```python no-exec — not executed: the wiring face (the bound's declared home is the @app.workflow decorator; verified: the backpressure pins — the captured red shows the unbounded variant blowing the in-flight count 8 > 6)
@wf_app.workflow("doc_screen_router", max_in_flight=6)  # the DECLARED bound
def doc_screen_router() -> Promise[object]:
    return build(chain_source(SCREEN_CHAIN, _screen_source_body, key="screen_source"))
    # the admission fence (in-tx advisory lock + bounded poll) admits
    # new children only under the cap; a page WIDER than the bound is
    # REFUSED loudly, and a fence-wait longer than the pager's term is
    # the ladder-owned stall, named.
```

The order of degradation when the bound is hit is the design point:
**observability degrades FIRST, correctness never.** An emission that
cannot land is coalesced (the progress emitter's discipline) and the
drop is ON THE RECORD (the drop accounting: appended == retained +
dropped); a child that cannot be admitted waits at the fence. The run
never lies and never materializes without bound. The stale-payload
dragon (a dashboard reading a progress row newer than the row it
describes) dies at the same seam: the terminal-mark's statement carries
a zero-finalize-changes probe.

## The windows: progress projection + the aggregate read

Event pipelines want dashboards: how far along is the window, how many
records settled. The two-channel persistence answers it:

- **the STATE channel** (`wf_node_progress`): one row per
  `(node_id, channel)`, upserted latest-wins — the row count is nodes ×
  channels, CONSTANT whatever the emission rate;
- **the STREAM channel** (`wf_node_stream`): append rows in a bounded
  per-node ring (64, drop-oldest, the dropped count on the record) —
  the "at 14:32" question's answer, one seq space for the body's
  emissions and the engine's projections.

The map's read-side aggregate is a DECLARED pure function over the
children's result rows, evaluated at read time — mid-flight, without
blocking the join:

```python no-exec — not executed: the declared aggregate's shape (verified: the read-side pins drive read_map_aggregate over a declared aggregate)
enriched = map_source(
    ingested,
    enrich_item,
    aggregate=lambda rows: {"readable": sum(1 for r in rows if isinstance(r, Summary))},
)
# the dashboard's read: taskq.workflows.read_map_aggregate(pool, schema, flow_id, parent_id)
```

This is deliberately NOT a windowed-aggregation engine: there are no
tumbling or sliding event-time windows, because there is one clock
domain (the database's). The loop + the gather + the aggregate read are
the documented recipe for the real cases. If your pipeline's core
question is event-time semantics, that is a stream processor's job —
this engine's niche is the durable orchestration AROUND it.

## The partial-success envelope: the failed record's report, named

When one record fails its ladder, the pipeline does not fail the run
silently, retry the world, or drop the record. The failure fans in as
part of the COLLECT — and the terminal report derives from it:

```python no-exec — not executed: verbatim fragment of examples/workflows.py (verified: tests/test_wf_demo_legs.py — property 1: doc-doomed fails its first attempt through its ladder; the failure surfaces in the report; the siblings never re-run)
class PublishReport(BaseModel):
    """The typed terminal verdict: the failures ride the report, named —
    DERIVED from the run's own collect (the envelope is the truth,
    never a hardcoded `[]`)."""

    published: list[str]
    dead_lettered: list[str]
    failed: list[str]
    review_note: str = ""


async def publish_body(
    ctx: Any,
    review_note: str,
    ready: list[str],
    labels: list[str],
    enriched: list[Summary | Unreadable],
) -> PublishReport:
    published = sorted(set(ready) & set(labels))
    dead_lettered = sorted(it.doc_id for it in enriched if isinstance(it, Unreadable))
    raw = ctx.input
    batch_ids = (
        IngestBatch.model_validate(raw).doc_ids
        if raw is not None
        else [it.doc_id for it in enriched]
    )
    failed = sorted(set(batch_ids) - set(published) - set(dead_lettered))
    return PublishReport(
        published=published,
        dead_lettered=dead_lettered,
        failed=failed,
        review_note=review_note,
    )
```

The three properties, on one real run:

1. **The failed record's ladder is per-item.** `doc-doomed` fails its
   first attempt (armed in `enrich_item`: `ctx.attempt < 2`); the retry
   re-runs THAT child alone — the siblings and the succeeded items
   never re-run. The map's ledger identity (per `map_index`) is what
   makes "alone" true at the claim level, not by convention.
2. **The envelope derives from the run's own collect.** `dead_lettered`
   names the `Unreadable` arm; `failed` is the batch's residual (a doc
   that neither published nor dead-lettered — a third state no honest
   pipeline may hide). Hardcoding `[]` would be the lie; the report is
   computed from the rows.
3. **The consumer `match`es the union with the exhaustiveness idiom**
   (`case _: assert_never(it)`) — a new outcome arm the envelope forgets
   is a checker error at every consumer.

The operator reads the same story without the code: `taskq flows status
<run_id>` names the failed node, its attempt ledger, and the remedy; the
admin's run explorer shows the failing child's attempts under the map
hexagon (addressed by `?map_index=N`) — see [Task stacks](task-stacks.md)
§the operator's day for the full surface.

## Where the limits are (honest)

- **No event-time windows.** One clock domain, deliberately. The
  loop + gather recipe is the answer; a stream processor composes
  upstream or beside.
- **The map forks all children of a page.** The admission bound
  (`max_in_flight=`) throttles execution; the rows exist. A page is
  yours to size.
- **Backpressure bounds are per-workflow, declared at wiring.** There
  is no global credit pool.

# DocShelf — the adoption story (a document pipeline on the flow engine)

DocShelf is a WORKED ADOPTION: a document-intelligence pipeline built,
drilled, and shipped as an example (`examples/docshelf/`) — documents
arrive from an upload feed, each document chunks (the map face), each
chunk's extraction calls a flaky/slow/hanging MOCK extraction API,
documents with many chunks spawn CHILD FLOWS for the expensive
sub-analyses (spawn + join), the per-document results aggregate, the
documents branch by class (the typed route: contracts → the summary
arm, invoices → the ledger arm, unknown → the quarantine arm), and the
shelf publishes a digest. A nightly re-index runs on the durable timer
(cron) with a durable cursor (only NEW documents re-process).

Everything external is MOCKED at the boundary; the TaskQ machinery is
REAL — real Postgres, real rows, real worker PROCESSES (the crash
drills SIGKILL them). Its drill suite (15 tests, the chaos suite runs
5 iterations) passes against real workers; the receipts (counts,
retries, slot measurements, the variance) land in
`.measurements/docshelf/`.

This page is the BUILD-THROUGH: the shapes an adopting team needs, in
the order the team reached for them, with the frictions the adoption
hit — filed with file:line, never patched around in the library.

## The five-minute tour (runnable)

A miniature of DocShelf's extraction map: documents in, the flaky mock
API per chunk, the fan-in aggregate out. The fence runs end-to-end
against a real Postgres (the same env vars every docs fence reads):

```python
import asyncio
import os

import asyncpg

import taskq.migrate
from examples.docshelf.mockapi import MockExtractionAPI, chunk_count
from examples.docshelf.config import AdverseDynamics
from examples.docshelf.models import ChunkRef, DocPayload, ExtractedChunk
from pydantic import BaseModel
from taskq.workflows import (
    Promise,
    StepContext,
    WorkflowApp,
    build,
    map_source,
    run,
    step,
)


class Shelf(BaseModel):
    extracted: int
    tokens: int


# The fence's mock runs with the adversity OFF (this tour shows the API
# shapes; the drills run the RECORDED profile — flake 20%, timeouts 7%).
API = MockExtractionAPI(AdverseDynamics(extract_flake_rate=0.0, extract_timeout_rate=0.0))


async def feed(ctx: StepContext) -> list[DocPayload]:
    # The upload feed's stand-in: three documents.
    return [DocPayload(doc_id=f"mini-{i}", content=f"the text of document {i} " * 9) for i in range(3)]


async def extract_doc(ctx: StepContext, doc: DocPayload) -> list[ExtractedChunk]:
    # THE MAP'S ITEM = one document; its chunks call the mock API
    # (flaky ~20%, slow 30-200 ms, hanging ~7% — the recorded profile).
    n = chunk_count(doc.doc_id, doc.content)
    out = []
    for idx in range(n):
        out.append(
            await API.call(
                ChunkRef(doc_id=doc.doc_id, idx=idx, text=f"{doc.content} [part {idx + 1}/{n}]",
                         chunk_key=f"{doc.doc_id}:{idx}"),
                ctx.attempt,
            )
        )
    return out


async def aggregate(ctx: StepContext, extracted: list[list[ExtractedChunk]]) -> Shelf:
    flat = [c for chunks in extracted for c in chunks]
    return Shelf(extracted=len(flat), tokens=sum(c.tokens for c in flat))


app = WorkflowApp()


@app.workflow("docshelf_mini")
def docshelf_mini() -> Promise[object]:
    docs = step(feed, key="feed")
    extracted = map_source(docs, extract_doc)
    return build(step(aggregate, extracted, key="aggregate"))


async def main() -> None:
    dsn = os.environ["TASKQ_PG_DSN"]
    schema = os.environ["TASKQ_SCHEMA_NAME"]
    await taskq.migrate.apply_pending_locked(dsn, schema=schema, phase="pre")
    await taskq.migrate.apply_pending_locked(dsn, schema=schema, phase="post")
    pool = await asyncpg.create_pool(dsn)
    outcome = await run(app.get("docshelf_mini"), pool, schema)
    print(f"{outcome.outcome}: {outcome.result}")
    await pool.close()


asyncio.run(main())
```

Verified output (this guide's capture, on a fresh schema):

```
terminal: {'tokens': 846, 'extracted': 18}
```

The map's item is the DOCUMENT — not the chunk. That is the adoption's
first scale lesson (F4/F6 below): a 500-doc batch chunks to ~2300
chunks, and the engine refuses fan-ins past 1000 declared parents —
`map_source` over the documents fans 500 children and the per-chunk
mock calls run INSIDE each item, exactly-once through `ctx.substep`.

## The shapes, and where each one earned its keep

| shape | where DocShelf uses it | the drill that proves it |
| --- | --- | --- |
| the map face | `map_source(ingested, extract_doc_chunks)` — the map over documents | the happy path: 50 docs → the fan-out's overlap measured from the rows |
| the fan-in | the map's derived join (`ingest.join`) | the flake drill: 2335 chunks, all collected, nothing lost |
| the typed route | `route(classified, {ContractDoc: ..., InvoiceDoc: ..., UnknownDoc: ...})` | the route drill: each class ran ONLY its arm |
| the child flow | `run_sub_analyses` spawns `docshelf_sub_analysis` per large doc via `FlowRunner.create_flow` + waits on ONE typed batch hold | the child drill: the slot law MEASURED on a one-slot worker |
| the slot law | the hold is a row: the parent's slot releases while the children run | the child drill's overlap: the children's 1-3 s executions INSIDE the parent's hold window |
| the collect | the sub flow's `map_source(..., on_failure="collect")` — the poison's failure fans in as the typed `FailureInfo` | the child drill: the child's terminal failure DELIVERED into the parent's typed failure arm |
| the exactly-once ledger | `ctx.substep(f"chunk:{idx}", ...)` — the SIGKILL's re-run REPLAYS the recorded extractions | the crash drills: zero double-extraction across a SIGKILL |
| the cross-run cache | `step(reprocess_doc, ..., cache=True)` | the cache drill: the producing run's receipt on the hit's node row; the API never re-called |
| the durable timer | `cron(...)` + the tick actor | the timer drill: the fire no earlier than `next_fire_at` |
| the durable cursor | `advance_cursor`'s GREATEST update | the timer drill: only the increment re-processed; the cursor never rewinds |
| the typed success receipt | `FlowRunner.result(...)` → the decoded `ShelfDigest` | every drill's final assertion |

## The child-flow face: spawn + join, in full

Subflows are NOT typed (the pattern catalog's recorded backlog): the
child edge is invisible to `validate()` and to the graph view. DocShelf
adopts the pattern with the ENGINE'S OWN primitives and makes the join
exact:

1. **The spawn** — the parent body calls `FlowRunner(child).create_flow(input=..., run_key=...)`. The run key (`docshelf:sub:{parent}:{doc}`) makes the spawn idempotent: a resumed body's re-create returns the SAME run.
2. **The wait** — the parent waits ONCE on a typed hold (`ctx.wait_signal((SubBatchDone,), timeout_s=..., reason=...)`). The hold is a row: the worker's slot releases while the children run.
3. **The delivery** — the child flow's TAIL records its typed outcome durably (an app table, idempotent by child run); the LAST tail to record assembles the batch and DELIVERS it into the parent's hold (`HitlClient.resolve`) — the resolve's CAS makes it exactly-once ('no-op' for the loser).
4. **The failure arm** — the child's absorbed failure (the collect's `FailureInfo` on the join row) rides the SAME delivery as the typed `SubFailed`: the parent's digest NAMES the failed child — never silent. A lost child (the hold expired) is the typed `Expired` arm — also named.
5. **The link** — the parent↔child link is a ROW (`docshelf_child_runs`): the operator's join between a run and its sub-analysis children.

THE SHAPE LAW the adoption measured (F5): the hold's identity is
(run, node key, signal, epoch) — concurrent waits on ONE node key
collide on the epoch (the measured UniqueViolationError), and a
wait-per-child burns an attempt row per hold. ONE batch hold + ONE
resume is the shape that works.

## The drills

```bash
docker run -d --name docshelf-pg -e POSTGRES_PASSWORD=taskq \
  -e POSTGRES_DB=taskq -p 5804:5432 postgres:17 -c max_connections=1000 -c fsync=off
export DOCSHELF_PG_DSN=postgresql://postgres:taskq@localhost:5804/taskq
uv run pytest examples/docshelf/tests -q --timeout=1200
```

The suite's own captured receipts (from the adoption run this page
documents — 15/15 green):

- **the happy path** (50 docs): every document in exactly one arm, the
  fan-out's effective concurrency measured > 2 (the overlap is real),
  the child flows' rows linked, the digest published once.
- **the flake drill** (500 docs, 5 pages): 2335 chunks; 3140 extraction
  attempts; **625 transient failures + 180 timeouts injected**; 586
  chunks needed a retry; max 6 attempts observed on one chunk; **0
  documents died**; zero double-extraction (the substep ledger).
- **the route drill**: per-class receipts — each document's element ran
  ONLY its class's arm.
- **the child drill**: on a ONE-slot worker the children ran INSIDE the
  parent's hold window (the measured overlap 1.2-1.8 s of a 6.2 s
  hold); the poison child's failure landed in the parent's typed
  failure arm.
- **the crash drills**: SIGKILL mid-extraction-fan-out AND
  mid-child-join — the restart resumed, nothing lost, nothing
  duplicated (zero re-extracted chunks).
- **the timer + cursor drill**: the tick enqueued at/after
  `next_fire_at`; a worker killed mid-timer-wait; the fire happened
  once; the second pass processed ONLY the increment (3 of 15); the
  cursor never rewound.
- **the duplicate-feed drill**: the re-delivered delivery returned the
  SAME run; the overlapping re-delivery processed ZERO documents; the
  register held each document exactly once.
- **the cache drill**: the re-run's node row named the PRODUCING run;
  the mock API's call count did not move.
- **the faces drill**: the admin's run SSE captured live (mid-run +
  terminal); the run pages + the child runs rendered on the explorer.
- **the chaos suite**: 5 iterations of flake+SIGKILL at a different
  kill delay each — every iteration's invariant held; the honest
  variance: the walls ran 10-65 s, the injected flakes 25-59 and the
  timeouts 6-17 per iteration, zero re-extractions.

## The frictions (the adoption's findings, filed — never patched)

| # | friction | evidence |
| --- | --- | --- |
| F1 | **The workflow seam's retry ladder exhausts at a hard 3** — regardless of the wiring's `max_attempts=` (the row is STAMPED with 8 and the ladder stops at 3: `examples/docshelf/app.py`, `_extract_inner`'s docstring), regardless of `@app.actor(..., max_attempts=9)` (the declaration is ignored by the projection), regardless of a manual `actor_config.max_attempts` UPDATE (the ladder never reads it), and the vanilla-registry carrier (a vanilla `@actor` sharing the cohort's name) is reverted at every boot by `--force-update-actor-config`. The vanilla path honors `RetryPolicy(max_attempts=8)` (attempts 1..8 observed). The docs promise "per-node max_attempts" (`docs/api-reference/workflows.md`, the `step` verb's signature; `docs/guides/workflows.md`'s retry knob). The adoption carried its own inner retry budget to compensate. | the ladder probes + the failed row: attempt=3, max_attempts=8, status=failed |
| F2 | **The boot projection never projects the TYPED ROUTE's arm cohorts** — the arms fork at RUNTIME (not compiled nodes), so `actor_config` rows for the arms' queues never exist and the arm rows strand (`stranded_jobs_no_actor_config`). The cure is the deployment's own provisioning (`examples/docshelf/provision.py`, called at `examples/docshelf/worker_entry.py`) — the migrate → workers → clients discipline, plus this registration step. | the flake drill's stranded warnings; the projection's blind face |
| F3 | **`route()`'s derived join defaults to queue `'default'` while inheriting the source's ACTOR** — a cohort conflict (one actor, two queues) surfaced ONLY at worker boot as `workflow-projection-skipped` (the whole workflow's cohorts dropped; the rows strand). `validate()` passes clean; the compile knows the cohorts. The cure: pass `queue=` to `route()` (`examples/docshelf/app.py`, the `routed = route(...)` wiring). | the boot log's WorkflowActorQueueConflictError |
| F4 | **A multi-parent consumer of a ROUTE join can dispatch before the join's terminal** — the engine's own assertion names it: "the parent 'classify.join' has no result yet — the arg resolution ran before the edge's terminal (a dispatch bug)". Deterministic solo at ~160-200 classified elements; at 100-element pages under multi-page concurrency it races (two faces: the assertion + a None-packed slot). The route join's single-consumer shape + the feed's SEQUENTIAL page delivery are the adoption's cures (`examples/docshelf/app.py`: the single-parent `publish_digest`, the paging `deliver_batch`). | the fire-race probes (100/150 OK; 160/180/200 failed) + the flake drill's failed pages |
| F5 | **A hold's identity is per NODE KEY, not per map child** — concurrent map children of one node key collide on the hold epoch (the measured `UniqueViolationError`), and a wait-per-child burns an attempt row per hold ('awaited' rows — the ladder not burned, the resume-not-retry law holding). The sanctioned shape: ONE waiting node, sequential waits or ONE batch hold. | the child drill's restructure evidence |
| F6 | **The 1000-parent fan-in bound is a REAL wall for chunk-level maps** — `FanInBoundExceededError` at 2335 children; the escape (`JoinSpec(child_driven=True)`) is not exposed on `map_source`. The docs' guidance (partition the map) is right; DocShelf partitions by document. | the flake drill's first run: the fork refused |
| F7 | **The in-process driver mishandles concurrent map-child holds** — `drive(until="held")` stops at the FIRST hold while sibling map children are mid-flight, leaving a stale claimed row the next drive cannot resolve (the max_ticks wedge). Fleet mode (real workers) is the clean path. | the adoption's first probe |
| F8 | **The admin's runs-list/run-explorer paths live under the router mount only** (`/admin/workflows/{run_id}`, `/admin/api/runs/{run_id}/stream`) — the docs' `/taskq/...` spelling is the standalone `taskq ui serve`'s base. And the parent↔child links of a subflow shape are NOT rendered (the recorded backlog; F-docs): the operator's join is the app's own rows (`docshelf_child_runs`) + the run-key convention. | the faces drill |
| F9 | **The multi-parent join's body receives RAW jsonb parents (dicts), not the declared models** — the decode-once law held at single-parent joins (the demo's bodies use attribute access) but the digest's `routed`/`verdicts` params arrived as dicts (the body re-validates at the boundary). | `publish_digest`'s re-validation block |
| F10 | **The substep's replay is the crash-recovery hero and deserves the docs' headline** — the SIGKILL's re-run replays the recorded chunk extractions at zero cost (the ledger's "attempt is NOT in that key" clause); the adoption's zero-double-extraction invariant is ASSERTABLE only through it. | the crash drills' receipts: zero re-extracted chunks |

## The verdict (the adopting team's own, after the drills)

See the adoption report's verdict section — the short form: the core
durability story is REAL (the crash drills' resume is flawless; the
exactly-once ledger is honest; the route's totality fence has teeth),
and the adoption-blocking findings are F1 (the ladder's hard 3), F4
(the multi-parent early fire) and F2 (the unprojected arm cohorts) —
each worked around at the app layer, none acceptable for a fleet that
cannot fork the library.

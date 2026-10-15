"""DocShelf's workflow app — the whole product's graphs, wired.

Four workflows on ONE ``WorkflowApp`` (one deps instance at the door):

* ``docshelf_batch`` — the main pipeline: ingest → chunk+extract (the
  map face over documents, the flaky mock API per chunk) → the fan-in
  aggregate → the child-flow face (large documents spawn CHILD FLOWS;
  the batch joins through ONE typed hold) → classify → the TYPED ROUTE
  (contracts → the summary arm, invoices → the ledger arm, unknown →
  the quarantine arm) → the digest (the completion report).
* ``docshelf_sub_analysis`` — the CHILD FLOW: the expensive sub-analysis
  (1-3 s per doc); its tail records the typed outcome durably and the
  LAST tail to finish delivers the batch payload into the parent's hold
  — success and failure both, never silent.
* ``docshelf_reprocess`` — a single-document re-run (the cross-run step
  cache's face: an unchanged document's second run never re-extracts).
* ``docshelf_reindex`` — the nightly re-index: the durable timer (cron)
  fires the tick actor; the flow processes only the cursor's increment.

The wiring verbs' ``queue=``/``actor=`` placement follows the engine's
law — one actor, one queue; a map's children inherit the SOURCE's actor
cohort, so each map's source step is stamped with its map's cohort.
"""

from __future__ import annotations

import asyncio

import asyncpg

from examples.docshelf import store
from examples.docshelf.config import DOCSHELF_QUEUES, DocShelfConfig
from examples.docshelf.deps import DocShelfDeps
from examples.docshelf.mockapi import (
    ExtractionExhaustedError,
    ExtractionTransientError,
    SubAnalysisBoomError,
    chunk_count,
    sub_analysis_duration_s,
)
from examples.docshelf.models import (
    ChunkRef,
    ContractDoc,
    ContractSummary,
    DeliveryBatch,
    DocPayload,
    DocRef,
    DocRefSeq,
    DocVerdict,
    DocWork,
    ExtractedChunk,
    IndexedDoc,
    InvoiceDoc,
    LedgerEntry,
    NightlyTick,
    NightlyTickResult,
    QuarantinedDoc,
    ReindexInput,
    ReindexReport,
    ShelfDigest,
    SubAnalysisIn,
    SubAnalysisReport,
    SubBatchDone,
    SubDone,
    SubFailed,
    SubRequest,
    UnknownDoc,
)
from taskq import actor, cron
from taskq.workflows import (
    Expired,
    FlowRunner,
    HitlClient,
    Promise,
    RouteArm,
    StepContext,
    WorkflowApp,
    build,
    map_source,
    route,
    step,
)

DOCSHELF_CONFIG = DocShelfConfig.load()

DEPS = DocShelfDeps()

SHELF_APP = WorkflowApp(deps=DEPS)


class ChildDeliveryLostError(Exception):
    """The child's typed outcome could not reach the parent's hold."""


# ── the batch flow's bodies ──────────────────────────────────────────────


async def feed_in(
    ctx: StepContext, params: DeliveryBatch | None, deps: DocShelfDeps
) -> list[DocPayload]:
    """The feed's ingest: IDEMPOTENT registration — a document is
    registered ONCE and processed ONCE, whoever delivers it. A
    re-delivered document (the feed is at-least-once) is named in the
    receipt's duplicates and does NOT flow downstream (the duplicate-feed
    drill's zero-double-effects starts here).

    The run's OWN input is the truth: ``deliver_batch`` carries the batch
    on ``create_flow(input=...)`` and the body reads ``ctx.input`` (the
    wiring's declared batch is the bare-create fallback)."""
    pool = await deps.pool()
    raw = ctx.input if ctx.input is not None else (params.model_dump() if params else None)
    if raw is None:
        return []
    batch = DeliveryBatch.model_validate(raw)
    new_docs, duplicates = await store.register_docs(pool, deps.schema, batch.docs)
    await ctx.progress(
        min(100, len(new_docs)),
        f"ingested {len(new_docs)} new documents ({len(duplicates)} duplicates deduped)",
        {"registered": len(new_docs), "duplicates": len(duplicates)},
    )
    return new_docs


async def _extract_inner(ctx: StepContext, chunk: ChunkRef, deps: DocShelfDeps) -> ExtractedChunk:
    """ONE chunk through the mock boundary under the app's inner budget
    (the fast absorber — a transient flake or timeout costs a cheap
    inner retry inside the SAME attempt, never a body re-run; the
    framework ladder above it honors the stamped budget — friction F1
    is CURED, see extract_doc_chunks' docstring)."""
    pool = await deps.pool()
    dyn = deps.cfg.dynamics
    last: Exception | None = None
    for inner in range(dyn.extract_inner_retries):
        await store.record_api_call(pool, deps.schema, chunk.chunk_key, ctx.attempt, "start")
        try:
            async with asyncio.timeout(dyn.extract_timeout_s):
                result = await deps.api.call(chunk, f"{ctx.attempt}.{inner}")
        except TimeoutError as exc:
            last = exc
            await store.record_api_call(pool, deps.schema, chunk.chunk_key, ctx.attempt, "timeout")
        except ExtractionTransientError as exc:
            last = exc
            await store.record_api_call(pool, deps.schema, chunk.chunk_key, ctx.attempt, "flake")
        else:
            await store.record_api_call(pool, deps.schema, chunk.chunk_key, ctx.attempt, "success")
            await ctx.progress(
                None,
                f"extracted {chunk.chunk_key}",
                {"chunk_key": chunk.chunk_key, "tokens": result.tokens, "inner_retries": inner},
            )
            return result
        await asyncio.sleep(0.03 + 0.02 * inner)
    raise ExtractionExhaustedError(
        f"the inner retry budget exhausted for {chunk.chunk_key}: {last}"
    )


async def extract_doc_chunks(
    ctx: StepContext, doc: DocPayload, deps: DocShelfDeps
) -> list[ExtractedChunk]:
    """THE MAP FACE, over documents: each document's chunks extract
    through the MOCK BOUNDARY — the flaky, slow, sometimes-hanging
    extraction API under the DECLARED timeout, per chunk.

    THE MAP'S GRANULARITY IS THE DOCUMENT (the partition law: a 500-doc
    batch fans 500 children — the chunk-level fan would fan ~2300 into
    ONE join and the engine refuses fan-ins past 1000; the document is
    the honest partition, the docs' own guidance for a map past the
    bound).

    THE EXACTLY-ONCE LEDGER: each chunk's extraction rides a SUBSTEP
    (the step ledger's own memo — ``ctx.substep``) — a reclaim after a
    SIGKILL re-runs this body and the SUCCEEDED chunks' substeps REPLAY
    their recorded results: the killed body's at-least-once boundary
    costs ZERO re-calls (the drills assert it: no chunk's mock API is
    ever called twice successfully).

    TWO retry layers, both recorded (the drills' witness is the durable
    API ledger):

    * the INNER budget (``extract_inner_retries``, the app's own): the
      flaky API's transient failures + timeouts are absorbed with a
      short jittered backoff INSIDE one attempt — a flake costs a cheap
      re-call, never a body re-run (the chunk's earlier chunks replay
      free through the substep ledger, but the re-run still re-enters
      the map body). The framework ladder HONORS ITS STAMP now (friction
      F1 is CURED — the holdmap lane's ladder-inheritance cure: the map
      item ladders to the wired ``max_attempts=``, pinned green), so the
      inner budget is a performance choice, not a compensation.
    * the FRAMEWORK ladder (the map's ladder): the layer for INFRA
      faults — a reclaim after a crash re-runs the attempt without
      burning it. The child drill's poison exercises the ladder itself.
    """
    n = chunk_count(doc.doc_id, doc.content)
    out: list[ExtractedChunk] = []
    for idx in range(n):
        chunk = ChunkRef(
            doc_id=doc.doc_id,
            idx=idx,
            text=f"{doc.content} [part {idx + 1}/{n}]",
            chunk_key=f"{doc.doc_id}:{idx}",
        )
        raw = await ctx.substep(f"chunk:{idx}", _extract_inner, ctx, chunk, deps)
        out.append(raw if isinstance(raw, ExtractedChunk) else ExtractedChunk.model_validate(raw))
    return out


async def mk_docwork(
    ctx: StepContext, extracted: list[list[ExtractedChunk]], deps: DocShelfDeps
) -> list[DocWork]:
    """Fan-in aggregate: the per-document extractions flatten back into
    per-document work bundles; documents with MANY chunks (>= the
    threshold) spawn child-flow sub-analyses."""
    work = [
        DocWork(
            doc_id=chunks[0].doc_id if chunks else "",
            n_chunks=len(chunks),
            needs_child=len(chunks) >= deps.cfg.many_chunks_threshold,
            chunks=chunks,
        )
        for chunks in extracted
    ]
    children = sum(1 for w in work if w.needs_child)
    await ctx.progress(
        min(100, children),
        f"{children} of {len(work)} documents spawn sub-analysis children",
        {"child_flows": children},
    )
    return work


async def run_sub_analyses(
    ctx: StepContext, work: list[DocWork], deps: DocShelfDeps
) -> list[DocVerdict]:
    """THE CHILD-FLOW FACE — spawn + join, the parent's slot RELEASED:

    the body spawns every child flow (run-key idempotent: a resumed
    body's re-create returns the SAME run), then waits ONCE on a TYPED
    batch hold. The hold is a row: the worker's slot releases while the
    children run (the slot law the child drill observes, not asserts).
    The LAST child tail to finish assembles every child's recorded
    outcome and delivers the BATCH payload into the hold — the resolve's
    CAS makes the delivery exactly-once (a second assembler's resolve is
    the defined no-op). THE SHAPE IS THE BATCH JOIN'S OWN DESIGN, NOT A
    COLLISION DODGE: the hold identity now carries the MAP INDEX
    (friction F5 is CURED — the holdmap lane's map-index identity:
    concurrent per-item holds on ONE node key are addressable and
    individually resolvable, pinned green), so a wait-per-child would
    no longer collide; the batch still joins through ONE hold, ONE
    resume because the parent consumes ONE batch answer."""
    pool = await deps.pool()
    parent = str(ctx.flow_id)
    runner = FlowRunner(SHELF_APP.get("docshelf_sub_analysis"), pool, deps.schema)

    # The DIRECT verdicts first (small documents: no child flow).
    verdicts: list[DocVerdict] = []
    children: list[DocWork] = []
    for req in work:
        if not req.needs_child:
            verdicts.append(_direct_verdict(req))
        else:
            children.append(req)

    # THE SPAWN: one child flow per large document.
    for req in children:
        claim = await runner.create_flow(
            input=SubAnalysisIn(
                parent_flow_id=parent,
                doc_id=req.doc_id,
                n_chunks=req.n_chunks,
                expected_siblings=len(children),
            ),
            run_key=f"docshelf:sub:{parent}:{req.doc_id}",
        )
        await store.record_child_link(pool, deps.schema, str(claim.flow_id), parent, req.doc_id)

    if children:
        # THE JOIN: ONE typed batch hold — the last child tail to finish
        # assembles every child's recorded outcome and delivers it.
        outcome = await ctx.wait_signal(
            (SubBatchDone,),
            timeout_s=deps.cfg.sub_wait_timeout_s,
            reason=f"docshelf:subs:{parent}",
        )
        if isinstance(outcome, Expired):
            # The children's deliveries never assembled (the worst
            # stall): EVERY child takes the typed failure arm — named,
            # never silent.
            verdicts.extend(
                DocVerdict(
                    doc_id=req.doc_id,
                    n_chunks=req.n_chunks,
                    child_joined=False,
                    spawned_child=True,
                    child_error_class="ChildLost",
                    child_error_message=(
                        "the batch's typed delivery never arrived (the hold expired)"
                    ),
                )
                for req in children
            )
        else:
            results = {r.doc_id: r for r in outcome.results}
            for req in children:
                item = results.get(req.doc_id)
                if item is None:
                    verdicts.append(
                        DocVerdict(
                            doc_id=req.doc_id,
                            n_chunks=req.n_chunks,
                            child_joined=False,
                            spawned_child=True,
                            child_error_class="ChildLost",
                            child_error_message=(
                                "the child's outcome was missing from the batch delivery"
                            ),
                        )
                    )
                elif isinstance(item, SubDone):
                    verdicts.append(
                        DocVerdict(
                            doc_id=req.doc_id,
                            n_chunks=req.n_chunks,
                            child_joined=True,
                            spawned_child=True,
                            tags=item.tags,
                            score=item.score,
                        )
                    )
                else:
                    verdicts.append(
                        DocVerdict(
                            doc_id=req.doc_id,
                            n_chunks=req.n_chunks,
                            child_joined=False,
                            spawned_child=True,
                            child_error_class=item.error_class,
                            child_error_message=item.message,
                        )
                    )
    return verdicts


def _direct_verdict(work: DocWork) -> DocVerdict:
    """A small document's verdict: no child flow — the sub-analysis's
    job (tags, score) resolves INLINE from the doc's own extracted
    chunks (the same mock compute, minus the child flow's overhead)."""
    entities: list[str] = []
    for chunk in work.chunks:
        entities.extend(chunk.entities)
    return DocVerdict(
        doc_id=work.doc_id,
        n_chunks=work.n_chunks,
        child_joined=True,
        spawned_child=False,
        tags=entities[:8],
        score=round(len(entities) / max(1, work.n_chunks), 4),
    )


async def classify_docs(
    ctx: StepContext, verdicts: list[DocVerdict], deps: DocShelfDeps
) -> list[ContractDoc | InvoiceDoc | UnknownDoc]:
    """The typed route's SOURCE: each document branches by class —
    contracts, invoices, unknown. The class is deterministic in the
    document's id (a mock classifier's stand-in)."""
    from examples.docshelf.mockapi import roll

    classified: list[ContractDoc | InvoiceDoc | UnknownDoc] = []
    for verdict in verdicts:
        r = roll("class", verdict.doc_id)
        if r < 0.45:
            classified.append(
                ContractDoc(
                    doc_id=verdict.doc_id,
                    n_chunks=verdict.n_chunks,
                    clauses=3 + int(roll("clauses", verdict.doc_id) * 12),
                    tags=verdict.tags,
                    spawned_child=verdict.spawned_child,
                    child_error_class=verdict.child_error_class,
                    child_error_message=verdict.child_error_message,
                )
            )
        elif r < 0.90:
            classified.append(
                InvoiceDoc(
                    doc_id=verdict.doc_id,
                    n_chunks=verdict.n_chunks,
                    total_cents=1000 + int(roll("cents", verdict.doc_id) * 99000),
                    tags=verdict.tags,
                    spawned_child=verdict.spawned_child,
                    child_error_class=verdict.child_error_class,
                    child_error_message=verdict.child_error_message,
                )
            )
        else:
            classified.append(
                UnknownDoc(
                    doc_id=verdict.doc_id,
                    n_chunks=verdict.n_chunks,
                    reason="the classifier's confidence was below threshold",
                    spawned_child=verdict.spawned_child,
                    child_error_class=verdict.child_error_class,
                    child_error_message=verdict.child_error_message,
                )
            )
    return classified


@SHELF_APP.actor(name="docshelf-summary", queue="docshelf-summary")
async def contract_summary_arm(
    ctx: StepContext, doc: ContractDoc, deps: DocShelfDeps
) -> ContractSummary:
    """The CONTRACT arm: the summary. Its element arrives AS the declared
    model — the route's decode law. The ARM'S PLACEMENT lives on the
    wiring's ``RouteArm(actor=..., queue=...)`` (the authoritative stamp
    the fork writes); the boot projection walks the arms' effective
    placements since the engine-cures lane's F2 cure — the arm's rows
    project at boot, never strand (the decorator's registration is the
    same pair, belt-and-suspenders)."""
    return ContractSummary(
        doc_id=doc.doc_id,
        n_chunks=doc.n_chunks,
        headline=f"{doc.clauses}-clause contract; tags: {doc.tags}",
        spawned_child=doc.spawned_child,
        child_error_class=doc.child_error_class,
        child_error_message=doc.child_error_message,
    )


@SHELF_APP.actor(name="docshelf-ledger", queue="docshelf-ledger")
async def ledger_arm(ctx: StepContext, doc: InvoiceDoc, deps: DocShelfDeps) -> LedgerEntry:
    """The INVOICE arm: the ledger entry (the placement lives on the
    wiring's RouteArm — see the summary arm)."""
    return LedgerEntry(
        doc_id=doc.doc_id,
        n_chunks=doc.n_chunks,
        total_cents=doc.total_cents,
        account="receivable",
        spawned_child=doc.spawned_child,
        child_error_class=doc.child_error_class,
        child_error_message=doc.child_error_message,
    )


@SHELF_APP.actor(name="docshelf-quarantine", queue="docshelf-quarantine")
async def quarantine_arm(ctx: StepContext, doc: UnknownDoc, deps: DocShelfDeps) -> QuarantinedDoc:
    """The UNKNOWN arm: the quarantine (the placement lives on the
    wiring's RouteArm — see the summary arm)."""
    return QuarantinedDoc(
        doc_id=doc.doc_id,
        n_chunks=doc.n_chunks,
        reason=doc.reason,
        spawned_child=doc.spawned_child,
        child_error_class=doc.child_error_class,
        child_error_message=doc.child_error_message,
    )


async def publish_digest(
    ctx: StepContext,
    routed: list[ContractSummary | LedgerEntry | QuarantinedDoc],
    deps: DocShelfDeps,
) -> ShelfDigest:
    """The completion report — the typed terminal verdict, the ROUTE
    JOIN's SINGLE consumer (a multi-parent join's arg resolution races
    the route join's terminal at scale — friction F4 — so the digest
    reads everything off the arms' outputs). The digest row publishes
    ONCE per delivery (idempotent upsert)."""
    raw = ctx.input
    delivery_id = DeliveryBatch.model_validate(raw).delivery_id if raw is not None else "unkeyed"
    # The typed boundary: re-validate the join's packed sum into the
    # declared models (the discriminator 'kind' decides the member).
    routed = [
        ContractSummary.model_validate(it)
        if it.get("kind") == "contract_summary"
        else LedgerEntry.model_validate(it)
        if it.get("kind") == "ledger_entry"
        else QuarantinedDoc.model_validate(it)
        for it in (r if isinstance(r, dict) else r.model_dump() for r in routed)
    ]
    contracts = sorted(it.doc_id for it in routed if isinstance(it, ContractSummary))
    ledger = sorted(it.doc_id for it in routed if isinstance(it, LedgerEntry))
    quarantined = sorted(it.doc_id for it in routed if isinstance(it, QuarantinedDoc))
    failed_children = sorted(it.doc_id for it in routed if it.child_error_class is not None)
    expired = sorted(it.doc_id for it in routed if it.child_error_class == "ChildLost")
    digest = ShelfDigest(
        delivery_id=delivery_id,
        contracts_published=contracts,
        ledger_entries=ledger,
        quarantined=quarantined,
        failed_children=failed_children,
        expired_children=expired,
        total_docs=len(routed),
        total_chunks=sum(it.n_chunks for it in routed),
        child_flows_spawned=sum(1 for it in routed if it.spawned_child),
    )
    pool = await deps.pool()
    await store.publish_digest_row(pool, deps.schema, delivery_id, digest.model_dump())
    return digest


# ── the child flow's bodies ──────────────────────────────────────────────


async def sub_tasks(ctx: StepContext, params: SubAnalysisIn | None = None) -> list[SubRequest]:
    """The child flow's source: the run's OWN input is the truth (the
    parent spawned this run with the doc's identity)."""
    raw = ctx.input if ctx.input is not None else (params.model_dump() if params else None)
    if raw is None:
        return []
    inp = SubAnalysisIn.model_validate(raw)
    return [SubRequest(doc_id=inp.doc_id, n_chunks=inp.n_chunks, needs_child=True)]


async def sub_analyze(ctx: StepContext, req: SubRequest, deps: DocShelfDeps) -> SubDone:
    """The expensive sub-analysis: 1-3 s (the recorded profile). Poison
    documents fail TERMINALLY (the child drill's armed failure)."""
    if req.doc_id.startswith(deps.cfg.poison_prefix):
        raise SubAnalysisBoomError(
            f"the sub-analysis of {req.doc_id} failed terminally (the drill's poison)"
        )
    await asyncio.sleep(sub_analysis_duration_s(req.doc_id, deps.cfg.dynamics))
    from examples.docshelf.mockapi import roll

    tags = [f"tag-{i}" for i in range(2 + int(roll("tags", req.doc_id) * 3))]
    return SubDone(doc_id=req.doc_id, tags=tags, score=round(roll("score", req.doc_id), 4))


async def sub_tail(
    ctx: StepContext, analyzed: list[SubDone], deps: DocShelfDeps
) -> SubAnalysisReport:
    """The child flow's JOIN TAIL: the child's typed outcome lands in the
    durable result rows; the LAST tail to record assembles the batch and
    DELIVERS the typed payload into the parent's ONE hold — the failures
    ride the SAME payload (a child's failure is never silent). The
    absorbed failures read from the join row's own record (the collect's
    bounded summary)."""
    pool = await deps.pool()
    input_raw = ctx.input
    inp = (
        SubAnalysisIn.model_validate(input_raw)
        if input_raw
        else SubAnalysisIn(parent_flow_id="", doc_id="", n_chunks=0, expected_siblings=1)
    )
    parent_flow_id, doc_id = inp.parent_flow_id, inp.doc_id

    failures = await store.join_failures(pool, deps.schema, str(ctx.flow_id), "sub_tasks.join")
    if failures:
        first = failures[0]
        payload: SubDone | SubFailed = SubFailed(
            doc_id=doc_id,
            error_class=str(first.get("error_class", "ChildAnalysisFailed")),
            message=str(first.get("error_message", "the sub-analysis failed (absorbed)")),
        )
        verdict = "failed"
    elif analyzed:
        payload = analyzed[0]
        verdict = "succeeded"
    else:
        payload = SubFailed(
            doc_id=doc_id, error_class="ChildEmpty", message="the sub-analysis produced nothing"
        )
        verdict = "empty"

    recorded = await store.record_sub_result(
        pool, deps.schema, str(ctx.flow_id), parent_flow_id, doc_id, payload.model_dump()
    )
    delivered = False
    if recorded >= inp.expected_siblings:
        # THE LAST ASSEMBLER: every sibling's outcome is on the rows.
        results = await store.assemble_sub_results(pool, deps.schema, parent_flow_id)
        hitl = HitlClient(pool, schema=deps.schema)
        holds = await hitl.list(run=parent_flow_id)
        mine = [h for h in holds if h.reason == f"docshelf:subs:{parent_flow_id}"]
        if mine:
            batch = SubBatchDone(
                results=[SubDone.model_validate(r) for r in results if r.get("kind") == "sub_done"]
                + [SubFailed.model_validate(r) for r in results if r.get("kind") == "sub_failed"]
            )
            result = await hitl.resolve(mine[0].hold_id, batch.model_dump())
            # 'delivered' = the resume fired; 'no-op' = the CAS lost to an
            # earlier deliver of the SAME batch (this tail re-ran) — the
            # exactly-once face. Both mean the parent has the answer.
            delivered = result.status in ("delivered", "no-op")
    if not delivered and recorded >= inp.expected_siblings:
        # Every sibling recorded but the hold never opened (or the
        # resolve kept refusing): LOUD — the child's own ladder takes
        # this; the parent's typed Expired arm is the backstop.
        raise ChildDeliveryLostError(
            f"the sub-analysis batch of {parent_flow_id} assembled but never delivered"
        )
    return SubAnalysisReport(doc_id=doc_id, delivered=delivered, verdict=verdict)


# ── the reprocess flow's body (the cross-run cache's face) ───────────────


async def unpack_ref(ctx: StepContext, ref: DocRef | None) -> DocRef:
    """THE UNCACHED UPSTREAM (E18's law): the run's OWN input is the
    truth (the wiring's DocRef is the bare-create fallback); the ref
    enters the DATAFLOW here so the cached step's address can see it —
    ``ctx.input`` never enters a cache address, the WIRED param does."""
    raw = ctx.input if ctx.input is not None else (ref.model_dump() if ref else None)
    if raw is None:
        raise ValueError("the reprocess run carries no document")
    return DocRef.model_validate(raw)


async def reprocess_doc(ctx: StepContext, ref: DocRef, deps: DocShelfDeps) -> IndexedDoc:
    """A single document's (re-)processing: chunk + extract + index.
    The wiring opts this step INTO the cross-run cache — an unchanged
    document's re-run never re-extracts (the cache drill measures it).
    The ref rides as a WIRED PARAM (lifted by the uncached unpack_ref
    step — E18's law: the flow input never enters the cache address,
    the wired arg does), so the address covers the document's identity
    honestly."""
    pool = await deps.pool()
    n = chunk_count(ref.doc_id, ref.content)
    tokens = 0
    for idx in range(n):
        result = await _extract_inner(
            ctx,
            ChunkRef(
                doc_id=ref.doc_id,
                idx=idx,
                text=f"{ref.content} [part {idx + 1}/{n}]",
                chunk_key=f"{ref.doc_id}:{idx}",
            ),
            deps,
        )
        tokens += result.tokens
    await store.upsert_index(pool, deps.schema, ref.doc_id, n, tokens)
    return IndexedDoc(doc_id=ref.doc_id, n_chunks=n, tokens=tokens)


# ── the nightly re-index flow's bodies ───────────────────────────────────


async def pending_docs(
    ctx: StepContext, params: ReindexInput | None, deps: DocShelfDeps
) -> list[DocRefSeq]:
    """The increment: documents registered after the CURSOR's seq — only
    NEW documents re-process (the durable cursor's read)."""
    raw = ctx.input if ctx.input is not None else (params.model_dump() if params else None)
    from_seq = ReindexInput.model_validate(raw).from_seq if raw else 0
    pool = await deps.pool()
    rows = await store.new_documents(pool, deps.schema, from_seq)
    await ctx.progress(len(rows), f"{len(rows)} new documents to re-index", {"pending": len(rows)})
    return [DocRefSeq(doc_id=d, seq=s) for d, s in rows]


async def reindex_doc(ctx: StepContext, ref: DocRefSeq, deps: DocShelfDeps) -> DocRefSeq:
    """One document's re-index: the index upsert is idempotent by doc."""
    pool = await deps.pool()
    content = await store.document_content(pool, deps.schema, ref.doc_id)
    if content is None:
        return ref
    n = chunk_count(ref.doc_id, content)
    tokens = max(1, len(content.split())) * n
    await store.upsert_index(pool, deps.schema, ref.doc_id, n, tokens)
    await ctx.progress(None, f"re-indexed {ref.doc_id}", {"doc_id": ref.doc_id})
    return ref


async def advance_cursor(
    ctx: StepContext, reindexed: list[DocRefSeq], deps: DocShelfDeps
) -> ReindexReport:
    """The durable cursor's advance — GREATEST: the cursor NEVER rewinds
    (a crash mid-flow leaves the cursor where it was; the re-run
    re-processes the increment and advances once)."""
    pool = await deps.pool()
    raw = ctx.input
    from_seq = ReindexInput.model_validate(raw).from_seq if raw else 0
    new_cursor = max((r.seq for r in reindexed), default=from_seq)
    final = await store.advance_cursor(pool, deps.schema, new_cursor)
    return ReindexReport(processed=len(reindexed), new_cursor=final)


# ── the wiring ───────────────────────────────────────────────────────────

_EMPTY_BATCH = DeliveryBatch(delivery_id="docshelf-demo", docs=[])


@SHELF_APP.workflow("docshelf_batch")
def docshelf_batch() -> Promise[object]:
    ingested = step(
        feed_in,
        _EMPTY_BATCH,
        key="ingest",
        # THE EXTRACTION MAP'S COHORT: the map's children inherit the
        # SOURCE's actor (one actor, one queue) — the ingest step rides
        # the extract cohort so the per-document extraction children do.
        actor="docshelf-extract",
        queue="docshelf-extract",
    )
    extracted = map_source(
        ingested,
        extract_doc_chunks,
        queue="docshelf-extract",
        max_attempts=DOCSHELF_CONFIG.dynamics.extract_max_attempts,
    )
    requests = step(
        mk_docwork,
        extracted,
        key="mk_docwork",
        actor="docshelf-child",
        queue="docshelf-child",
    )
    verdicts = step(
        run_sub_analyses,
        requests,
        key="run_children",
        actor="docshelf-child",
        queue="docshelf-child",
        max_attempts=3,
    )
    classified = step(
        classify_docs, verdicts, key="classify", actor="docshelf-cpu", queue="docshelf-cpu"
    )
    routed = route(
        classified,
        {
            ContractDoc: RouteArm(
                body=contract_summary_arm, actor="docshelf-summary", queue="docshelf-summary"
            ),
            InvoiceDoc: RouteArm(body=ledger_arm, actor="docshelf-ledger", queue="docshelf-ledger"),
            UnknownDoc: RouteArm(
                body=quarantine_arm, actor="docshelf-quarantine", queue="docshelf-quarantine"
            ),
        },
        # THE ROUTE'S JOIN PLACEMENT: the derived fan-back-in node rides
        # the classify cohort (queue='default' would conflict the
        # inherited actor across two queues — a boot-time discovery only,
        # so name it HERE).
        queue="docshelf-cpu",
    )
    digest = step(
        publish_digest,
        routed,
        key="digest",
        actor="docshelf-digest",
        queue="docshelf-digest",
    )
    return build(digest)


@SHELF_APP.workflow("docshelf_sub_analysis")
def docshelf_sub_analysis() -> Promise[object]:
    tasks = step(
        sub_tasks,
        SubAnalysisIn(parent_flow_id="", doc_id="", n_chunks=0),
        key="sub_tasks",
        actor="docshelf-child",
        queue="docshelf-child",
    )
    analyzed = map_source(
        tasks, sub_analyze, queue="docshelf-child", on_failure="collect", max_attempts=2
    )
    return build(
        step(sub_tail, analyzed, key="sub_tail", actor="docshelf-child", queue="docshelf-child")
    )


@SHELF_APP.workflow("docshelf_reprocess")
def docshelf_reprocess() -> Promise[object]:
    ref = step(
        unpack_ref,
        DocRef(doc_id="doc-demo", content="demo content"),
        key="load_ref",
        actor="docshelf-cpu",
        queue="docshelf-cpu",
    )
    return build(
        step(
            reprocess_doc,
            ref,
            key="index_doc",
            actor="docshelf-cpu",
            queue="docshelf-cpu",
            cache=True,
            cache_ttl=3600.0,
        )
    )


@SHELF_APP.workflow("docshelf_reindex")
def docshelf_reindex() -> Promise[object]:
    pending = step(
        pending_docs,
        ReindexInput(from_seq=0),
        key="pending",
        actor="docshelf-cpu",
        queue="docshelf-cpu",
    )
    reindexed = map_source(pending, reindex_doc, queue="docshelf-cpu", max_attempts=3)
    return build(
        step(
            advance_cursor,
            reindexed,
            key="advance_cursor",
            actor="docshelf-cpu",
            queue="docshelf-cpu",
        )
    )


# ── the nightly timer: the cron schedule + the tick actor ────────────────


@actor(name="docshelf-nightly-tick", queue="docshelf-cron")
async def docshelf_nightly_tick(payload: NightlyTick) -> NightlyTickResult:
    """The schedule's arm: read the durable cursor, fire the re-index
    flow keyed BY the cursor (the same slot twice → ONE run — the
    run-key arbiter's claim; a redelivered tick cannot double-fire)."""
    pool = await DEPS.pool()
    cursor = await store.read_cursor(pool, DEPS.schema)
    runner = FlowRunner(SHELF_APP.get("docshelf_reindex"), pool, DEPS.schema)
    claim = await runner.create_flow(
        input=ReindexInput(from_seq=cursor),
        run_key=f"docshelf:reindex:{cursor}",
    )
    return NightlyTickResult(flow_id=str(claim.flow_id), run_status=str(claim.status))


cron(
    DOCSHELF_CONFIG.reindex_cron,
    "docshelf-nightly-tick",
    static_payload=NightlyTick(slot="nightly").model_dump(),
    name="nightly-reindex",
)


# ── the feed (the mock upload boundary) ──────────────────────────────────


async def deliver_batch(pool: asyncpg.Pool, schema: str, batch: DeliveryBatch) -> list[str]:
    """THE MOCK FEED'S DELIVERY: at-least-once — the feed keys each run
    by the DELIVERY id (+ the page), so a re-delivered delivery cannot
    double-run (the run-key arbiter's claim; the duplicate-feed drill
    exercises it).

    THE PAGE (the adoption's measured scale law, its honest residual):
    one flow's ROUTE fan is safe to ~150 classified elements SOLO — past
    it, AND under multi-page concurrency, the route join's
    fire/arg-resolution raced its consumer (friction F4). The in-process
    face is clean since the engine-cures lane's re-probe (3 rounds x
    200 elements, all green) and the connection-hold cure landed (the
    holdmap lane's pool pin — a substep no longer pins the pool
    connection across the body); the FLEET face was never re-convicted
    and never disproven, so the feed still pages its deliveries AND
    delivers the pages ONE AT A TIME (each page's run reaches terminal
    before the next page fires). The zero-loss invariant is per DOCUMENT
    across the pages. A single-page delivery returns without waiting
    (the crash drills' shape: the flow must be RUNNING while the drill
    kills the worker).

    Returns one flow id per page."""
    page_size = DOCSHELF_CONFIG.delivery_page_docs
    runner = FlowRunner(SHELF_APP.get("docshelf_batch"), pool, schema)
    page_count = max(1, (len(batch.docs) + page_size - 1) // page_size)
    flow_ids: list[str] = []
    for i in range(page_count):
        page_docs = batch.docs[i * page_size : (i + 1) * page_size]
        claim = await runner.create_flow(
            input=DeliveryBatch(delivery_id=batch.delivery_id, docs=page_docs),
            run_key=f"docshelf:delivery:{batch.delivery_id}:p{i}",
        )
        flow_ids.append(str(claim.flow_id))
        if page_count > 1:
            # The sequential-page discipline (multi-page deliveries only).
            deadline = asyncio.get_event_loop().time() + 600
            while asyncio.get_event_loop().time() < deadline:
                row = await pool.fetchrow(
                    f'SELECT status FROM "{schema}".jobs WHERE id = $1',  # noqa: S608  # Why: the interpolated value is the app's settings-validated schema identifier; every caller-controlled value is $N-bound.
                    claim.flow_id,
                )
                if row and row["status"] in ("succeeded", "failed", "cancelled"):
                    break
                await asyncio.sleep(0.5)
            else:
                raise RuntimeError(f"page {i} of {batch.delivery_id} never reached terminal")
    return flow_ids


__all__ = [
    "DEPS",
    "DOCSHELF_CONFIG",
    "DOCSHELF_QUEUES",
    "SHELF_APP",
    "deliver_batch",
    "docshelf_nightly_tick",
]

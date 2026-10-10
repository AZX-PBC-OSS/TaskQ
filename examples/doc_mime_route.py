"""THE DOCUMENT SYNC PIPELINE — the mime-type router LIVE (T27's worked
example).

The maintainer's use case, end to end with the generic names (the OSS
naming law — no consumer/product names):

    ingest (the doc refs) ── the mime DETECTION (each element becomes the
    typed doc its mime implies) ──> THE TYPED ROUTE (the union's members
    key the arms) ──> per-type processing (the text extraction on the cpu
    queue, the OCR on the gpu queue — R4's per-arm placement) ──> THE
    FAN-IN AT THE CHUNK STEP (the sync barrier: the chunk fires ONCE,
    only when ALL the routed elements' texts have landed) ──> enrich.

The three things the example SHOWS:

1. THE MIME-TYPE ROUTER: the source detects each ref's mime and emits
   the TYPED element (``TextDoc | ImageDoc | UnsupportedDoc``) — the
   element's MODEL is the mime's tag, the mime STRING rides it for the
   record. ``route(docs, {TextDoc: ..., ImageDoc: ..., UnsupportedDoc:
   ...})`` keys the arms by the union's MEMBER TYPES; an element of a
   type with no arm is a BUILD refusal (the totality fence — the route
   covers the union or it does not compile).
2. THE PER-ARM PLACEMENT: ``RouteArm(body=..., queue=...)`` stamps each
   arm's children's queue — the text extraction's children run on the
   cpu queue, the OCR's on the gpu queue (the OCR is the SLOW arm; the
   rows are the receipt).
3. FORK, FAN BACK IN AT THE JOIN: the arms each emit the extracted text
   per element (``ExtractedText``); the derived join (``docs.join``) is
   the SYNC BARRIER — the chunk step consumes the routed elements'
   COLLECTED LIST (``chunk(texts: list[ExtractedText | DeadLettered])``)
   and fires ONCE, only when the LAST element's text has landed (the
   deps_pending count = the elements; the staggered arms — the text
   fast, the OCR slow — the chunk fires after the OCR's last child).
   The unsupported mime's element takes the dead-letter arm: the
   envelope records it (the flow lives — the typed sum carries the
   dead-letter item and the chunk's report names it).

Run: ``uv run python examples/doc_mime_route.py`` (needs a Postgres; see
README.md), or read it as the tour — every snippet in the guide's
"document sync pipeline" section is VERBATIM from this file.
"""

from __future__ import annotations

import asyncio
from typing import Any

from pydantic import BaseModel

from taskq.workflows import (
    FlowRunner,
    Promise,
    RouteArm,
    StepContext,
    WorkflowApp,
    build,
    route,
    step,
)

# ── the elements: the mime's tag IS the type ────────────────────────────


class DocRef(BaseModel):
    """One document ref (the ingest's input — the corpus's row)."""

    doc_id: str
    uri: str


class TextDoc(BaseModel):
    """A TEXT mime (text/plain, text/markdown, text/html, application/json)
    — the element the detection emits for it."""

    doc_id: str
    uri: str
    mime: str


class ImageDoc(BaseModel):
    """An IMAGE mime (image/png, image/tiff, image/jpeg) — the OCR's input."""

    doc_id: str
    uri: str
    mime: str


class UnsupportedDoc(BaseModel):
    """A mime the pipeline does not process — the dead-letter arm's input
    (the envelope records it; the flow lives)."""

    doc_id: str
    mime: str


class ExtractedText(BaseModel):
    """The arms' COMMON product: the processed text of one element —
    the typed sum's extracted member (what the chunk barrier consumes)."""

    doc_id: str
    text: str


class DeadLettered(BaseModel):
    """The dead-letter arm's product — the sum's OTHER member (the
    envelope: the doc's ref + its mime + the reason)."""

    doc_id: str
    mime: str
    reason: str


class Chunks(BaseModel):
    """The chunk barrier's product (the chunk sets, both arms' texts)."""

    doc_ids: list[str]
    chunk_count: int
    dead: list[str]


class IndexReport(BaseModel):
    """The enrich step's product (the flow's answer)."""

    indexed: int
    dead: list[str]


# ── the bodies ──────────────────────────────────────────────────────────

#: The demo corpus (the consumer's sync-pipeline source stands in here —
#: the refs ARE the rows; a real deployment reads them from the store).
CORPUS: dict[str, str] = {
    "doc-1": "text/plain",
    "doc-2": "image/png",
    "doc-3": "text/markdown",
    "doc-4": "image/tiff",
    "doc-5": "application/x-unknown",
}

_TEXT_MIMES = ("text/plain", "text/markdown", "text/html", "application/json")
_IMAGE_MIMES = ("image/png", "image/tiff", "image/jpeg")


async def sync_source(ctx: StepContext) -> list[TextDoc | ImageDoc | UnsupportedDoc]:
    """THE DOC SOURCE (the map over the document refs — the route's own
    fan-out IS the per-element machinery): each ref's mime is DETECTED
    and the element becomes the TYPED doc the mime implies — the type IS
    the tag; the mime string rides for the record."""
    return [
        TextDoc(doc_id=doc_id, uri=f"s3://docs/{doc_id}", mime=mime)
        if mime in _TEXT_MIMES
        else ImageDoc(doc_id=doc_id, uri=f"s3://docs/{doc_id}", mime=mime)
        if mime in _IMAGE_MIMES
        else UnsupportedDoc(doc_id=doc_id, mime=mime)
        for doc_id, mime in sorted(CORPUS.items())
    ]


async def extract_text(ctx: Any, item: TextDoc) -> ExtractedText:
    """THE TEXT ARM — the narrowed arm (the param declares ``TextDoc``:
    the decode's target IS this annotation). Runs on the CPU queue; the
    extraction is fast."""
    _ = ctx
    return ExtractedText(doc_id=item.doc_id, text=f"the text of {item.doc_id} ({item.mime})")


async def ocr_text(ctx: Any, item: ImageDoc) -> ExtractedText:
    """THE IMAGE ARM — the OTHER narrowed arm (``item: ImageDoc``). Runs
    on the GPU queue; the OCR is the SLOW arm (the real OCR is the
    consumer's — this body shows the SEAM: the call site, awaited; the
    barrier's honesty is proven against the stall)."""
    _ = ctx
    await asyncio.sleep(0.15)  # the OCR's latency, scaled for the demo
    return ExtractedText(doc_id=item.doc_id, text=f"the OCR of {item.doc_id} ({item.mime})")


async def dead_letter(ctx: Any, item: UnsupportedDoc) -> DeadLettered:
    """THE DEAD-LETTER ARM — the unsupported mime's envelope (the flow
    lives; the sum carries the dead-letter item)."""
    _ = ctx
    return DeadLettered(doc_id=item.doc_id, mime=item.mime, reason="unsupported mime")


async def chunk(ctx: Any, texts: list[ExtractedText | DeadLettered]) -> Chunks:
    """THE CHUNK BARRIER — the fan-in AT the chunking step: the body's
    param is the routed elements' COLLECTED LIST, DECODED at the join
    boundary (the typed sum — not dicts; attribute access works). The
    join fires ONCE, only when ALL the routed elements have landed (the
    all-members semantics: the deps_pending count = the elements; the
    OCR's stall cannot make the chunk fire early or twice)."""
    _ = ctx
    extracted = [t for t in texts if isinstance(t, ExtractedText)]
    dead = [d for d in texts if isinstance(d, DeadLettered)]
    return Chunks(
        doc_ids=[t.doc_id for t in extracted],
        chunk_count=len(extracted) * 3,  # the chunker's own seam
        dead=[f"{d.doc_id} ({d.mime}: {d.reason})" for d in dead],
    )


async def enrich(ctx: Any, chunks: Chunks) -> IndexReport:
    """THE ENRICH STEP — the chunk sets from BOTH arms, indexed (the
    dead letters named, never silently dropped)."""
    _ = ctx
    return IndexReport(indexed=chunks.chunk_count, dead=chunks.dead)


# ── the wiring ──────────────────────────────────────────────────────────

mime_app = WorkflowApp()


@mime_app.workflow("doc_mime_route")
def doc_mime_route() -> Promise[object]:
    """The document sync pipeline, spelled by dataflow: the source → the
    mime-type route (the per-arm placement) → THE CHUNK BARRIER (the
    fan-in) → the enrich."""
    docs = step(sync_source, key="docs")
    routed = route(
        docs,
        {
            TextDoc: RouteArm(body=extract_text, queue="cpu"),
            ImageDoc: RouteArm(body=ocr_text, queue="gpu"),
            UnsupportedDoc: RouteArm(body=dead_letter),
        },
    )
    chunks = step(
        chunk, routed, key="chunk"
    )  # THE FAN-IN: the chunk consumes the join's collected list
    return build(step(enrich, chunks, key="enrich"))


async def main() -> None:
    """The one-call run (the packaged door): the flow, driven to
    terminal, the report read."""
    import os

    dsn = os.environ.get("TASKQ_PG_DSN", "postgresql://postgres:taskq@localhost:5774/taskq")
    schema = os.environ.get("TASKQ_PG_SCHEMA", "public")
    import asyncpg

    pool = await asyncpg.create_pool(dsn)
    assert pool is not None
    runner = FlowRunner(mime_app.get("doc_mime_route"), pool, schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id) == "terminal"
    report = await runner.result(flow_id)
    print(f"the document sync pipeline's report: {report}")
    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())

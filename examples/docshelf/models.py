"""DocShelf's typed payload contract.

Every union member carries a ``Literal`` discriminator field (the
engine's union-decode law: the round-trip decodes by the tag, never by
field-count luck) and the members stand apart on their REQUIRED fields,
so the left-to-right smart union always falls through to the true member.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel

# ── the feed's shapes ────────────────────────────────────────────────────


class DocPayload(BaseModel):
    """One delivered document: an id and its text content."""

    doc_id: str
    content: str


class DeliveryBatch(BaseModel):
    """One feed delivery — at-least-once: the feed may re-deliver the
    SAME delivery (same id) or overlapping documents under a new id."""

    delivery_id: str
    docs: list[DocPayload]


class IngestReceipt(BaseModel):
    """The ingest step's receipt: what registered, what was a duplicate."""

    delivery_id: str
    registered: list[str]
    duplicates: list[str]


# ── chunking + extraction ───────────────────────────────────────────────


class ChunkRef(BaseModel):
    """One chunk of one document (the map's item identity)."""

    doc_id: str
    idx: int
    text: str
    chunk_key: str


class ExtractedChunk(BaseModel):
    """The mock extraction API's typed result."""

    kind: Literal["extracted"] = "extracted"
    doc_id: str
    idx: int
    chunk_key: str
    tokens: int
    entities: list[str]


class DocWork(BaseModel):
    """One document's post-extraction bundle: the document's chunks are
    all extracted (the map's item = the DOCUMENT — a 500-doc batch fans
    500 children, inside the engine's 1000-parent join bound; the chunk
    fan-out lives INSIDE each item as the mock API's per-chunk calls)."""

    doc_id: str
    n_chunks: int
    needs_child: bool
    chunks: list[ExtractedChunk]


# ── the child-flow face: spawn + join ───────────────────────────────────


class SubRequest(BaseModel):
    """One document's processing plan: a large document spawns a CHILD
    FLOW (needs_child); a small one's verdict resolves directly."""

    doc_id: str
    n_chunks: int
    needs_child: bool = False


class SubDone(BaseModel):
    """The child flow's typed SUCCESS delivery (into the parent's hold)."""

    kind: Literal["sub_done"] = "sub_done"
    doc_id: str
    tags: list[str]
    score: float


class SubFailed(BaseModel):
    """The child flow's typed FAILURE delivery — a child's failure lands
    HERE in the parent, never silent."""

    kind: Literal["sub_failed"] = "sub_failed"
    doc_id: str
    error_class: str
    message: str


class DocVerdict(BaseModel):
    """One document's post-extraction verdict (children joined or not)."""

    doc_id: str
    n_chunks: int
    child_joined: bool
    spawned_child: bool = False
    tags: list[str] = []
    score: float = 0.0
    child_error_class: str | None = None
    child_error_message: str | None = None


# ── the typed route's union + arms ──────────────────────────────────────


class ContractDoc(BaseModel):
    kind: Literal["contract"] = "contract"
    doc_id: str
    n_chunks: int = 0
    clauses: int
    tags: list[str]
    # The document's child-flow outcome rides the element (the digest
    # reads it off the arms' outputs — the route join's typed sum is the
    # ONE parent the digest consumes).
    spawned_child: bool = False
    child_error_class: str | None = None
    child_error_message: str | None = None


class InvoiceDoc(BaseModel):
    kind: Literal["invoice"] = "invoice"
    doc_id: str
    n_chunks: int = 0
    total_cents: int
    tags: list[str]
    spawned_child: bool = False
    child_error_class: str | None = None
    child_error_message: str | None = None


class UnknownDoc(BaseModel):
    kind: Literal["unknown"] = "unknown"
    doc_id: str
    reason: str
    n_chunks: int = 0
    spawned_child: bool = False
    child_error_class: str | None = None
    child_error_message: str | None = None


# ── the route arms' results + the digest ────────────────────────────────


class ContractSummary(BaseModel):
    kind: Literal["contract_summary"] = "contract_summary"
    doc_id: str
    headline: str
    n_chunks: int = 0
    spawned_child: bool = False
    child_error_class: str | None = None
    child_error_message: str | None = None


class LedgerEntry(BaseModel):
    kind: Literal["ledger_entry"] = "ledger_entry"
    doc_id: str
    total_cents: int
    account: str
    n_chunks: int = 0
    spawned_child: bool = False
    child_error_class: str | None = None
    child_error_message: str | None = None


class QuarantinedDoc(BaseModel):
    kind: Literal["quarantine"] = "quarantine"
    doc_id: str
    reason: str
    n_chunks: int = 0
    spawned_child: bool = False
    child_error_class: str | None = None
    child_error_message: str | None = None


class ShelfDigest(BaseModel):
    """The completion report — the typed terminal verdict of a batch."""

    delivery_id: str
    contracts_published: list[str]
    ledger_entries: list[str]
    quarantined: list[str]
    failed_children: list[str]
    expired_children: list[str]
    total_docs: int
    total_chunks: int
    child_flows_spawned: int


# ── the child flow's own shapes ─────────────────────────────────────────


class SubAnalysisIn(BaseModel):
    """The child flow's input: the parent's identity + the doc's chunks
    + the batch's shape (how many siblings the parent spawned — the LAST
    tail to finish assembles and delivers the batch's typed payload).

    The parent's flow id rides the INPUT (the run carries its input;
    bodies read ctx.input) — the parent↔child link is DATA, queryable
    from the rows, never a process-local fact."""

    parent_flow_id: str
    doc_id: str
    n_chunks: int
    expected_siblings: int = 1


class SubAnalysisReport(BaseModel):
    """The child flow's terminal result."""

    doc_id: str
    delivered: bool
    verdict: str


class SubBatchDone(BaseModel):
    """The BATCH payload the last-finishing child tail delivers into the
    parent's ONE hold: every child's typed outcome, assembled from the
    durable result rows."""

    kind: Literal["sub_batch_done"] = "sub_batch_done"
    results: list[SubDone | SubFailed]


# ── the reprocess flow (the cross-run step cache's face) ────────────────


class DocRef(BaseModel):
    """One document to (re)process — the cache address's input."""

    doc_id: str
    content: str


class IndexedDoc(BaseModel):
    doc_id: str
    n_chunks: int
    tokens: int


# ── the nightly re-index ────────────────────────────────────────────────


class ReindexInput(BaseModel):
    from_seq: int


class DocRefSeq(BaseModel):
    doc_id: str
    seq: int


class ReindexReport(BaseModel):
    processed: int
    new_cursor: int


class NightlyTick(BaseModel):
    """The cron schedule's static payload."""

    slot: str = "nightly"


class NightlyTickResult(BaseModel):
    """The tick actor's receipt: the run the schedule fired (or found)."""

    flow_id: str
    run_status: str

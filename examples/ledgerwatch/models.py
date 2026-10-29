"""LedgerWatch's typed payload contract.

THE DISCRIMINATOR LAW, learned the hard way and recorded here: every
union member carries a REQUIRED ``kind: Literal[...]`` tag. A tag with a
DEFAULT is an optional field — the validator reads only required fields
when it proves a union decodable, and field-identical required faces are
the build refusal. The tag is also what makes the smart union decode BY
TAG instead of by field-count luck.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel


class TransactionRecord(BaseModel):
    """One transaction from the feed (the generic textbook shape)."""

    txn_id: str
    amount_cents: int
    merchant: str
    channel: str  # "web" | "pos" | "ach" | ...


class Batch(BaseModel):
    """The feed's delivery: a batch of transactions, addressed by id.

    The batch id IS the pipeline's idempotency key: an at-least-once
    feed re-delivers batches, and the run-key arbiter turns the second
    delivery into the FIRST run's claim.
    """

    batch_id: str
    transactions: list[TransactionRecord]


# ── the enrichment's union (the branch's input) ─────────────────────────


class HighRiskTxn(BaseModel):
    """The enrichment verdict: needs human approval."""

    kind: Literal["high"]
    txn_id: str
    amount_cents: int
    score: float
    reasons: list[str]


class ClearableTxn(BaseModel):
    """The enrichment verdict: auto-clearable."""

    kind: Literal["clearable"]
    txn_id: str
    amount_cents: int
    score: float


EnrichedTxn = HighRiskTxn | ClearableTxn


# ── the main pipeline's verdicts (the route arms' returns) ──────────────


class ClearedTxn(BaseModel):
    """The clear arm: below the threshold, the machine's word is final."""

    kind: Literal["cleared"]
    txn_id: str


class PendingReviewTxn(BaseModel):
    """The high-risk arm: the transaction's review run is OPEN (the
    human approval holds THERE — the per-txn run is the hold's address;
    the batch's pipeline continues)."""

    kind: Literal["pending_review"]
    txn_id: str
    review_run_id: str


RoutingVerdict = ClearedTxn | PendingReviewTxn


# ── the HITL contract (the review run's hold) ───────────────────────────


class ReviewRequest(BaseModel):
    """The review run's input: ONE transaction to review."""

    txn_id: str
    batch_id: str
    amount_cents: int
    score: float
    reasons: list[str] = []


class ApprovalDecision(BaseModel):
    """The human's answer (the hold's declared payload)."""

    approved: bool
    reviewer: str = ""
    note: str = ""


class ReconTick(BaseModel):
    """The reconciliation timer's (optional) delivered payload.

    The timer's NORMAL face is the hold's expiry (``Expired``) — the
    delivered member is an operator's early nudge, which the body
    enforces against the schedule (never fires early).
    """

    note: str = ""


# ── the review run's verdicts (the review workflow's terminal) ──────────


class ApprovedTxn(BaseModel):
    kind: Literal["approved"]
    txn_id: str
    reviewer: str = ""


class RejectedTxn(BaseModel):
    kind: Literal["rejected"]
    txn_id: str
    note: str = ""


class EscalatedTxn(BaseModel):
    kind: Literal["escalated"]
    txn_id: str
    to_queue: str = "compliance"


ReviewVerdict = ApprovedTxn | RejectedTxn | EscalatedTxn


# ── the terminals ────────────────────────────────────────────────────────


class CompletionReport(BaseModel):
    """The typed terminal receipt of one batch's run.

    DERIVED from the run's own collect (the envelope is the truth):
    ``failed`` is the batch's residual — a transaction that produced no
    verdict — and its presence means the run did NOT go cleanly. The
    counts' law the tests assert: cleared + pending_review + failed ==
    total, and every id list is unique.
    """

    batch_id: str
    total: int
    cleared: list[str]
    pending_review: list[str]
    failed: list[str]

    @property
    def clean(self) -> bool:
        return not self.failed


class ReconReport(BaseModel):
    """The reconciliation pass's receipt (the durable timer's product)."""

    pass_id: str
    runs_total: int
    runs_succeeded: int
    runs_failed: int
    runs_running: int
    pending_holds: int
    escalated_total: int

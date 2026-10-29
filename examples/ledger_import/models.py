"""The typed wire models for the bulk ledger import.

Every value that crosses a node boundary is a pydantic model: the
route's arms dispatch on the ELEMENT's type, so the union
``ValidRow | InvalidRow`` is the validation branch's declaration, and
the run's decoded result is the receipt.
"""

from datetime import date

from pydantic import BaseModel, Field

# The intake rules. A record that breaks any of them is an invalid row:
# it quarantines with the machine-readable reasons, it never reaches the
# ledger table.
CURRENCIES = ("USD", "EUR", "GBP", "JPY")
ACCOUNT_PATTERN = r"^[A-Z]{2}\d{6}$"
MEMO_MAX_CHARS = 140


class RunInput(BaseModel):
    """The run's input: which feed, keyed by its content hash.

    The content hash rides the run input so the partition step's
    cross-run cache is content-keyed: a different feed is a different
    input, never a stale hit. (The run's own key is deliberately NOT
    part of the run input — it varies per run, and the cached step's
    address must not.)
    """

    path: str
    sha256: str
    chunk_size: int = Field(default=200, ge=1)


class LedgerRecord(BaseModel):
    """One raw feed record, as the feed file carries it."""

    entry_id: str
    account: str
    amount: float
    currency: str
    memo: str
    posted_at: date


class TransformedRow(BaseModel):
    """One row out of the partition step's transform."""

    chunk_index: int
    raw: dict[str, object]
    content_hash: str


class EntryChunk(BaseModel):
    """One partition of the feed: the map face's element."""

    chunk_index: int
    rows: list[TransformedRow]


class ValidRow(BaseModel):
    """A row the intake rules accepted."""

    chunk_index: int
    row: LedgerRecord
    content_hash: str


class InvalidRow(BaseModel):
    """A row the intake rules refused."""

    chunk_index: int
    entry_id: str | None
    raw: dict[str, object]
    reasons: list[str]


class ValidBatch(BaseModel):
    """A chunk's accepted rows, batched — the upsert arm's element."""

    chunk_index: int
    rows: list[ValidRow]


class InvalidBatch(BaseModel):
    """A chunk's refused rows, batched — the quarantine arm's element."""

    chunk_index: int
    rows: list[InvalidRow]


class RowOutcome(BaseModel):
    """What one route-arm job did: the fan-in's element."""

    kind: str  # "imported" | "updated" | "unchanged" | "quarantined"
    entry_id: str | None
    chunk_index: int
    reasons: list[str] = Field(default_factory=list)


class LedgerReceipt(BaseModel):
    """The run's typed completion receipt.

    The counts are the import's contract: ``imported + updated +
    unchanged + quarantined == total_rows``, and a re-import of the
    same feed reports ``imported == 0`` and ``updated == 0``.
    """

    run_key: str
    total_rows: int
    imported: int
    updated: int
    unchanged: int
    quarantined: int
    chunks: int
    quarantine_reasons: dict[str, int] = Field(default_factory=dict)
    sample_quarantined: list[str] = Field(default_factory=list)

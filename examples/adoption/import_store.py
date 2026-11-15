# ruff: noqa: S608, N818  # Why: identifiers interpolate only a validated lowercase scratch schema plus module-constant table names; $N binds every value. N818 keeps the finite domain-failure vocabulary readable (same convention as the accepted provider fixture).
"""Generic application-owned staging and reconciliation domain store.

This module is **fixture/sample support**, not an engine feature. It provides a
small typed domain API for a durable importer/reconciler:

* staging is explicit, inert, run-scoped, bounded, and isolated;
* every staging/settlement query is keyed by exact ``(flow, source, epoch)``;
* ``stage_page`` and ``commit_reconciliation`` take a caller-supplied actual
  ``asyncpg.Connection`` and require the caller's already-open transaction;
* this module never acquires a pool connection, opens a nested transaction, or
  commits/rolls back the caller's unit;
* the only pool-taking method is :meth:`ImportStore.initialize`, which is the
  explicit fixture bootstrap step.

The store deliberately contains no TaskQ workflow/engine import. Root status is
never consulted: an engine root becoming terminal is not application closure.
There is no staged-row release/execution path, no external exactly-once claim,
and no worker/sample runner. An at-least-once application callback may call
``commit_reconciliation`` again: the stable ``(flow, source, run_epoch,
barrier)`` settlement marker makes the same-database effects completed-once and
a duplicate call a provable no-op. External notifications remain outside this
module and are at-least-once.

Staging law
-----------
A page is appended only at the exact next ordinal with the exact cursor left by
the previous page. The same page replayed with identical cursor, ordinals,
content, and digest is a no-op; any divergent replay fails loudly. ``staged_*``
fields are never an applied watermark. ``commit_reconciliation`` consumes only
the run's own complete dataset, revalidates every page/record digest and
statistic against the stored bytes, then applies mirror upserts, explicit
anti-join deletes, append-only history, the applied watermark, the settlement
marker, and the run-settled state in the caller's one transaction.

Identity is never derived by hashing or modulo: ordinals are exact contiguous
positions and ``ProviderRecord.record_id`` is the logical record identity. The
SHA-256 values used here are content-integrity digests only, not identity.

Deletion protection
-------------------
``commit_reconciliation`` requires a caller-supplied explicit ``max_deletions``
bound. There is no permissive default. A candidate count above the bound raises
before any domain write, so the caller's transaction can still commit the
unmodified domain. An ordinary empty import is a valid no-op only when it
deletes nothing; the same empty import over existing mirror rows is refused
unless the caller explicitly authorizes enough deletions.

Quarantine
----------
``quarantine`` marks a failed/cancelled/poisoned/superseded run. Its staging
rows are physically retained for bounded inspection but can never be consumed
by a later run or epoch. Opening a later epoch requires the previous epoch to be
settled or quarantined and uses an explicit expected-current-epoch CAS.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import re
import secrets
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Final, Literal, cast

import asyncpg
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

from examples.adoption.provider import (
    MAX_RECORD_CONTENT_BYTES,
    MAX_RECORD_ID_LENGTH,
    MAX_RECORD_REVISION,
    ProviderRecord,
)

MAX_FLOW_LENGTH: Final[int] = 128
MAX_SOURCE_LENGTH: Final[int] = 128
MAX_BARRIER_LENGTH: Final[int] = 128
MAX_CURSOR_TEXT: Final[int] = 512
MAX_REASON_LENGTH: Final[int] = 128
MAX_EPOCH: Final[int] = (1 << 63) - 1
MAX_ORDINAL: Final[int] = (1 << 63) - 1

DEFAULT_MAX_PAGE_RECORDS: Final[int] = 100
HARD_MAX_PAGE_RECORDS: Final[int] = 10_000
DEFAULT_MAX_PAGE_BYTES: Final[int] = 256 * 1024
HARD_MAX_PAGE_BYTES: Final[int] = 16 * 1024 * 1024
DEFAULT_MAX_RECORDS: Final[int] = 10_000
HARD_MAX_RECORDS: Final[int] = 200_000
DEFAULT_MAX_TOTAL_BYTES: Final[int] = 64 * 1024 * 1024
HARD_MAX_TOTAL_BYTES: Final[int] = 512 * 1024 * 1024
DEFAULT_MAX_PAGES: Final[int] = 10_001
HARD_MAX_PAGES: Final[int] = HARD_MAX_RECORDS + 1
HARD_MAX_DELETIONS: Final[int] = 200_000

_NAME_PATTERN: Final[str] = r"^[A-Za-z0-9._:-]{1,128}$"
_SCHEMA_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[a-z][a-z0-9_]{0,62}$")
_DIGEST_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[0-9a-f]{64}$")
_RECORD_ID_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[A-Za-z0-9._:-]+$")

_EMPTY_RUN_DIGEST: Final[str] = hashlib.sha256(b"adoption-import-run-v1\0").hexdigest()

_T_CURRENT: Final[str] = "adoption_import_current"
_T_RUN: Final[str] = "adoption_import_run"
_T_PAGE: Final[str] = "adoption_import_stage_page"
_T_RECORD: Final[str] = "adoption_import_stage_record"
_T_MIRROR: Final[str] = "adoption_import_mirror"
_T_HISTORY: Final[str] = "adoption_import_history"
_T_WATERMARK: Final[str] = "adoption_import_watermark"
_T_SETTLEMENT: Final[str] = "adoption_import_settlement"

type QuarantineReason = Literal["failed", "cancelled", "poison", "superseded"]
type RunState = Literal["staging", "settled", "quarantined"]
type HistoryKind = Literal["upsert", "delete"]

_EXPECTED_COLUMNS: Final[dict[str, frozenset[str]]] = {
    _T_CURRENT: frozenset({"flow", "source", "current_epoch", "updated_at"}),
    _T_RUN: frozenset(
        {
            "flow",
            "source",
            "run_epoch",
            "state",
            "staged_pages",
            "staged_records",
            "staged_bytes",
            "staged_digest",
            "staged_through_cursor",
            "staged_complete",
            "quarantine_reason",
            "created_at",
            "updated_at",
            "settled_barrier",
            "settled_at",
            "quarantined_at",
        }
    ),
    _T_PAGE: frozenset(
        {
            "flow",
            "source",
            "run_epoch",
            "page_no",
            "from_cursor",
            "to_cursor",
            "ordinal_start",
            "record_count",
            "payload_bytes",
            "page_digest",
            "staged_at",
        }
    ),
    _T_RECORD: frozenset(
        {
            "flow",
            "source",
            "run_epoch",
            "ordinal",
            "page_no",
            "record_id",
            "revision",
            "content",
            "record_digest",
            "payload_bytes",
        }
    ),
    _T_MIRROR: frozenset(
        {"flow", "source", "record_id", "revision", "content", "applied_epoch", "updated_at"}
    ),
    _T_HISTORY: frozenset(
        {
            "seq",
            "flow",
            "source",
            "run_epoch",
            "barrier",
            "kind",
            "record_id",
            "ordinal",
            "revision",
            "content",
            "recorded_at",
        }
    ),
    _T_WATERMARK: frozenset(
        {
            "flow",
            "source",
            "applied_epoch",
            "applied_through_ordinal",
            "applied_cursor",
            "updated_at",
        }
    ),
    _T_SETTLEMENT: frozenset(
        {
            "flow",
            "source",
            "run_epoch",
            "barrier",
            "page_count",
            "record_count",
            "payload_bytes",
            "content_digest",
            "upserted_count",
            "deleted_count",
            "history_count",
            "applied_through_ordinal",
            "applied_cursor",
            "settled_at",
        }
    ),
}

_EXPECTED_TYPES: Final[dict[str, dict[str, str]]] = {
    _T_CURRENT: {
        "flow": "text",
        "source": "text",
        "current_epoch": "bigint",
        "updated_at": "timestamp with time zone",
    },
    _T_RUN: {
        "flow": "text",
        "source": "text",
        "run_epoch": "bigint",
        "state": "text",
        "staged_pages": "bigint",
        "staged_records": "bigint",
        "staged_bytes": "bigint",
        "staged_digest": "text",
        "staged_through_cursor": "text",
        "staged_complete": "boolean",
        "quarantine_reason": "text",
        "created_at": "timestamp with time zone",
        "updated_at": "timestamp with time zone",
        "settled_barrier": "text",
        "settled_at": "timestamp with time zone",
        "quarantined_at": "timestamp with time zone",
    },
    _T_PAGE: {
        "flow": "text",
        "source": "text",
        "run_epoch": "bigint",
        "page_no": "bigint",
        "from_cursor": "text",
        "to_cursor": "text",
        "ordinal_start": "bigint",
        "record_count": "bigint",
        "payload_bytes": "bigint",
        "page_digest": "text",
        "staged_at": "timestamp with time zone",
    },
    _T_RECORD: {
        "flow": "text",
        "source": "text",
        "run_epoch": "bigint",
        "ordinal": "bigint",
        "page_no": "bigint",
        "record_id": "text",
        "revision": "numeric",
        "content": "text",
        "record_digest": "text",
        "payload_bytes": "bigint",
    },
    _T_MIRROR: {
        "flow": "text",
        "source": "text",
        "record_id": "text",
        "revision": "numeric",
        "content": "text",
        "applied_epoch": "bigint",
        "updated_at": "timestamp with time zone",
    },
    _T_HISTORY: {
        "seq": "bigint",
        "flow": "text",
        "source": "text",
        "run_epoch": "bigint",
        "barrier": "text",
        "kind": "text",
        "record_id": "text",
        "ordinal": "bigint",
        "revision": "numeric",
        "content": "text",
        "recorded_at": "timestamp with time zone",
    },
    _T_WATERMARK: {
        "flow": "text",
        "source": "text",
        "applied_epoch": "bigint",
        "applied_through_ordinal": "bigint",
        "applied_cursor": "text",
        "updated_at": "timestamp with time zone",
    },
    _T_SETTLEMENT: {
        "flow": "text",
        "source": "text",
        "run_epoch": "bigint",
        "barrier": "text",
        "page_count": "bigint",
        "record_count": "bigint",
        "payload_bytes": "bigint",
        "content_digest": "text",
        "upserted_count": "bigint",
        "deleted_count": "bigint",
        "history_count": "bigint",
        "applied_through_ordinal": "bigint",
        "applied_cursor": "text",
        "settled_at": "timestamp with time zone",
    },
}


type _KeySpec = tuple[Literal["p", "u"], tuple[str, ...]]
_KEY_CONSTRAINTS: Final[dict[str, tuple[_KeySpec, ...]]] = {
    _T_CURRENT: (("p", ("flow", "source")),),
    _T_RUN: (("p", ("flow", "source", "run_epoch")),),
    _T_PAGE: (
        ("p", ("flow", "source", "run_epoch", "page_no")),
        ("u", ("flow", "source", "run_epoch", "ordinal_start")),
    ),
    _T_RECORD: (
        ("p", ("flow", "source", "run_epoch", "ordinal")),
        ("u", ("flow", "source", "run_epoch", "record_id")),
        ("u", ("flow", "source", "run_epoch", "page_no", "ordinal")),
    ),
    _T_MIRROR: (("p", ("flow", "source", "record_id")),),
    _T_HISTORY: (
        ("p", ("seq",)),
        ("u", ("flow", "source", "run_epoch", "barrier", "kind", "record_id")),
    ),
    _T_WATERMARK: (("p", ("flow", "source")),),
    _T_SETTLEMENT: (
        ("p", ("flow", "source", "run_epoch", "barrier")),
        ("u", ("flow", "source", "run_epoch")),
    ),
}

_CHECK_CONSTRAINTS: Final[dict[str, frozenset[str]]] = {
    _T_CURRENT: frozenset(
        {
            "adoption_import_current_flow_len_check",
            "adoption_import_current_source_len_check",
            "adoption_import_current_epoch_check",
        }
    ),
    _T_RUN: frozenset(
        {
            "adoption_import_run_epoch_check",
            "adoption_import_run_state_check",
            "adoption_import_run_staged_pages_check",
            "adoption_import_run_staged_records_check",
            "adoption_import_run_staged_bytes_check",
            "adoption_import_run_digest_check",
            "adoption_import_run_quarantine_reason_check",
            "adoption_import_run_settled_marker_check",
        }
    ),
    _T_PAGE: frozenset(
        {
            "adoption_import_page_epoch_check",
            "adoption_import_page_no_check",
            "adoption_import_page_ordinal_check",
            "adoption_import_page_record_count_check",
            "adoption_import_page_payload_check",
            "adoption_import_page_digest_check",
            "adoption_import_page_cursor_progress_check",
            "adoption_import_page_nonempty_check",
        }
    ),
    _T_RECORD: frozenset(
        {
            "adoption_import_record_epoch_check",
            "adoption_import_record_ordinal_check",
            "adoption_import_record_page_no_check",
            "adoption_import_record_id_len_check",
            "adoption_import_record_revision_check",
            "adoption_import_record_content_check",
            "adoption_import_record_digest_check",
            "adoption_import_record_payload_check",
        }
    ),
    _T_MIRROR: frozenset(
        {
            "adoption_import_mirror_id_len_check",
            "adoption_import_mirror_revision_check",
            "adoption_import_mirror_content_check",
            "adoption_import_mirror_epoch_check",
        }
    ),
    _T_HISTORY: frozenset(
        {
            "adoption_import_history_epoch_check",
            "adoption_import_history_kind_check",
            "adoption_import_history_revision_check",
            "adoption_import_history_ordinal_check",
        }
    ),
    _T_WATERMARK: frozenset(
        {
            "adoption_import_watermark_epoch_check",
            "adoption_import_watermark_ordinal_check",
        }
    ),
    _T_SETTLEMENT: frozenset(
        {
            "adoption_import_settlement_epoch_check",
            "adoption_import_settlement_page_count_check",
            "adoption_import_settlement_record_count_check",
            "adoption_import_settlement_payload_check",
            "adoption_import_settlement_digest_check",
            "adoption_import_settlement_upserted_check",
            "adoption_import_settlement_deleted_check",
            "adoption_import_settlement_history_count_check",
            "adoption_import_settlement_applied_ordinal_check",
            "adoption_import_settlement_history_sum_check",
        }
    ),
}


def _schema_text(value: object, *, name: str) -> str:
    raw = _runtime_value(value)
    if not isinstance(raw, str):
        raise ImportSchemaError(f"catalog {name} is not text")
    return raw


def _schema_int(value: object, *, name: str, minimum: int = 1) -> int:
    raw = _runtime_value(value)
    if isinstance(raw, bool) or type(raw) is not int or raw < minimum:
        raise ImportSchemaError(f"catalog {name} is not an exact bounded integer")
    return raw


def _schema_bool(value: object, *, name: str) -> bool:
    raw = _runtime_value(value)
    if not isinstance(raw, bool):
        raise ImportSchemaError(f"catalog {name} is not boolean")
    return raw


def _schema_attnums(value: object) -> tuple[int, ...]:
    raw = _runtime_value(value)
    if not isinstance(raw, list):
        raise ImportSchemaError("catalog constraint key is not an attribute list")
    return tuple(
        _schema_int(item, name="constraint attribute number", minimum=1)
        for item in cast("list[object]", raw)
    )


def _runtime_value(value: object) -> object:
    """Erase static narrowing for deliberate fail-closed runtime validation."""
    return value


class ImportStoreError(Exception):
    """Base class for this fixture's typed domain failures."""


class ImportContractError(ImportStoreError):
    """Malformed caller input, invalid bound, or unsupported value."""


class ImportSchemaError(ImportStoreError):
    """The scratch fixture schema could not be bootstrapped or is incompatible."""


class ImportTransactionRequired(ImportStoreError):
    """A staging/settlement method was called outside the caller's transaction."""


class ImportStoreCorrupt(ImportStoreError):
    """Persisted fixture state is internally inconsistent (not a caller replay)."""


class ImportRunMissing(ImportStoreError):
    """No current run pointer exists for the exact flow/source."""


class ImportRunNotOpen(ImportStoreError):
    """The run exists but is quarantined or its source is already sealed."""


class ImportRunActive(ImportStoreError):
    """A newer epoch was requested while the current run was still staging."""


class ImportEpochCASFailed(ImportStoreError):
    """The explicit expected-current-epoch compare-and-swap did not match."""


class ImportStaleEpoch(ImportStoreError):
    """The run epoch is no longer the current owner of this flow/source."""

    def __init__(self, run_epoch: int, current_epoch: int | None) -> None:
        self.run_epoch = run_epoch
        self.current_epoch = current_epoch
        super().__init__(f"run epoch {run_epoch} is not current (current is {current_epoch!r})")


class ImportAlreadySettled(ImportStoreError):
    """The run already has a different settlement barrier."""


class ImportSourceIncomplete(ImportStoreError):
    """The source has not staged a terminal page yet."""


class ImportStagingDiverged(ImportStoreError):
    """An exact-ordinal replay differs in cursor, content, statistics, or digest."""


class ImportCursorMismatch(ImportStoreError):
    """A page cursor does not continue the exact staged checkpoint."""

    def __init__(self, expected: str | None, observed: str | None) -> None:
        self.expected = expected
        self.observed = observed
        super().__init__(f"page cursor {observed!r} does not continue {expected!r}")


class ImportOrdinalGap(ImportStoreError):
    """A page starts beyond the next exact staged ordinal."""

    def __init__(self, expected: int, observed: int) -> None:
        self.expected = expected
        self.observed = observed
        super().__init__(f"page ordinal {observed} does not start at {expected}")


class ImportCorruptStaging(ImportStoreError):
    """Staged content/statistics/digest failed exact revalidation."""


class ImportBoundExceeded(ImportStoreError):
    """A page or retained run exceeds its declared finite bound."""

    def __init__(self, bound: str, limit: int, observed: int) -> None:
        self.bound = bound
        self.limit = limit
        self.observed = observed
        super().__init__(f"{bound} bound {limit} exceeded by {observed}")


class ImportMassDeleteRefused(ImportStoreError):
    """The anti-join candidate count exceeds the caller's explicit bound."""

    def __init__(self, observed: int, limit: int) -> None:
        self.observed = observed
        self.limit = limit
        super().__init__(
            f"mass deletion refused: {observed} mirror rows would be deleted, "
            f"explicit max_deletions is {limit}"
        )


class ImportConcurrentMutation(ImportStoreError):
    """The mirror changed between anti-join inspection and explicit deletion."""


class ImportWatermarkRegression(ImportStoreError):
    """The applied watermark would move backwards or split an epoch tie."""


class ImportIdentity(BaseModel):
    """Exact source/flow run identity; epoch is separate from every record id."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    flow: str = Field(min_length=1, max_length=MAX_FLOW_LENGTH, pattern=_NAME_PATTERN, strict=True)
    source: str = Field(
        min_length=1, max_length=MAX_SOURCE_LENGTH, pattern=_NAME_PATTERN, strict=True
    )
    run_epoch: int = Field(ge=1, le=MAX_EPOCH, strict=True)


class ImportPage(BaseModel):
    """One exact source page supplied by the application from its provider."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    cursor: str | None = Field(default=None, min_length=1, max_length=MAX_CURSOR_TEXT)
    next_cursor: str | None = Field(default=None, min_length=1, max_length=MAX_CURSOR_TEXT)
    has_more: bool = Field(strict=True)
    ordinal_start: int = Field(ge=0, le=MAX_ORDINAL, strict=True)
    records: tuple[ProviderRecord, ...] = Field(default=())

    @field_validator("cursor", "next_cursor")
    @classmethod
    def _cursor_is_strict_text(cls, value: str | None) -> str | None:
        raw = _runtime_value(value)
        if raw is not None and not isinstance(raw, str):
            raise ValueError("cursor must be exact text or None")
        return raw

    @model_validator(mode="after")
    def _page_is_coherent(self) -> ImportPage:
        if self.has_more and self.next_cursor is None:
            raise ValueError("a nonterminal page requires next_cursor")
        if not self.has_more and self.next_cursor is not None:
            raise ValueError("a terminal page must not carry next_cursor")
        if self.has_more and not self.records:
            raise ValueError("a nonterminal page cannot be empty")
        if self.next_cursor is not None and self.next_cursor == self.cursor:
            raise ValueError("a page cannot advance to the same non-null cursor")
        if len(self.records) > HARD_MAX_PAGE_RECORDS:
            raise ValueError("page exceeds HARD_MAX_PAGE_RECORDS")
        ids = [record.record_id for record in self.records]
        if len(ids) != len(set(ids)):
            raise ValueError("page contains duplicate record_id values")
        if self.ordinal_start + len(self.records) > MAX_ORDINAL:
            raise ValueError("page ordinal range exceeds the exact bigint bound")
        return self

    @property
    def payload_bytes(self) -> int:
        return sum(len(_record_canonical_bytes(record)) for record in self.records)


class StageProgress(BaseModel):
    """Observable staging progress; never an applied watermark."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    identity: ImportIdentity
    staged_pages: int = Field(ge=0, strict=True)
    staged_records: int = Field(ge=0, strict=True)
    staged_bytes: int = Field(ge=0, strict=True)
    staged_digest: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    staged_through_cursor: str | None = Field(default=None, max_length=MAX_CURSOR_TEXT)
    complete: bool = Field(strict=True)


class ImportRunState(BaseModel):
    """Typed read view of one staged/settled/quarantined run."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    identity: ImportIdentity
    state: RunState
    staged_pages: int = Field(ge=0, strict=True)
    staged_records: int = Field(ge=0, strict=True)
    staged_bytes: int = Field(ge=0, strict=True)
    staged_digest: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    staged_through_cursor: str | None = Field(default=None, max_length=MAX_CURSOR_TEXT)
    staged_complete: bool
    quarantine_reason: str | None = Field(default=None, max_length=MAX_REASON_LENGTH)
    settled_barrier: str | None = Field(default=None, max_length=MAX_BARRIER_LENGTH)
    settled_at: datetime | None = None
    quarantined_at: datetime | None = None
    created_at: datetime
    updated_at: datetime


class ImportWatermark(BaseModel):
    """Committed application watermark; the only applied-cursor authority."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    flow: str
    source: str
    applied_epoch: int = Field(ge=1, strict=True)
    applied_through_ordinal: int = Field(ge=-1, strict=True)
    applied_cursor: str | None = Field(default=None, max_length=MAX_CURSOR_TEXT)
    updated_at: datetime


class ImportSettlement(BaseModel):
    """One stable logical-barrier settlement marker and its exact statistics."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    identity: ImportIdentity
    barrier: str = Field(min_length=1, max_length=MAX_BARRIER_LENGTH, pattern=_NAME_PATTERN)
    page_count: int = Field(ge=1, strict=True)
    record_count: int = Field(ge=0, strict=True)
    payload_bytes: int = Field(ge=0, strict=True)
    content_digest: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    upserted_count: int = Field(ge=0, strict=True)
    deleted_count: int = Field(ge=0, strict=True)
    history_count: int = Field(ge=0, strict=True)
    applied_through_ordinal: int = Field(ge=-1, strict=True)
    applied_cursor: str | None = Field(default=None, max_length=MAX_CURSOR_TEXT)
    settled_at: datetime


class CommitResult(BaseModel):
    """Settlement plus whether this call replayed an already committed marker."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    settlement: ImportSettlement
    replayed: bool


@dataclass(frozen=True, slots=True)
class _RunRow:
    identity: ImportIdentity
    state: RunState
    staged_pages: int
    staged_records: int
    staged_bytes: int
    staged_digest: str
    staged_through_cursor: str | None
    staged_complete: bool
    quarantine_reason: str | None
    settled_barrier: str | None
    settled_at: datetime | None
    quarantined_at: datetime | None
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class _PageRow:
    page_no: int
    from_cursor: str | None
    to_cursor: str | None
    ordinal_start: int
    record_count: int
    payload_bytes: int
    page_digest: str


@dataclass(frozen=True, slots=True)
class _RecordRow:
    ordinal: int
    page_no: int
    record: ProviderRecord
    record_digest: str
    payload_bytes: int


@dataclass(frozen=True, slots=True)
class _ValidatedDataset:
    pages: tuple[_PageRow, ...]
    records: tuple[_RecordRow, ...]
    content_digest: str
    applied_through_ordinal: int
    applied_cursor: str | None


@dataclass(frozen=True, slots=True)
class _MirrorRow:
    record_id: str
    revision: int
    content: str


def _validated_schema(value: str) -> str:
    raw = _runtime_value(value)
    if not isinstance(raw, str) or _SCHEMA_PATTERN.fullmatch(raw) is None:
        raise ImportContractError("schema must be a lowercase scratch identifier")
    if raw in {"public", "information_schema"} or raw.startswith("pg_"):
        raise ImportContractError("schema must be a private scratch namespace")
    return raw


def _bounded_int(
    value: object,
    *,
    name: str,
    minimum: int,
    maximum: int,
) -> int:
    raw = _runtime_value(value)
    if isinstance(raw, bool) or type(raw) is not int:
        raise ImportContractError(f"{name} must be an exact int")
    if not minimum <= raw <= maximum:
        raise ImportContractError(f"{name} must be in [{minimum}, {maximum}]")
    return raw


def _validated_identity(identity: object) -> ImportIdentity:
    raw = _runtime_value(identity)
    if not isinstance(raw, ImportIdentity):
        raise ImportContractError("identity must be an ImportIdentity")
    try:
        return ImportIdentity.model_validate(raw.model_dump())
    except ValidationError as exc:
        raise ImportContractError(f"invalid import identity: {exc}") from exc


def _validated_page(page: object) -> ImportPage:
    raw = _runtime_value(page)
    if not isinstance(raw, ImportPage):
        raise ImportContractError("page must be an ImportPage")
    try:
        checked = ImportPage.model_validate(raw.model_dump())
    except ValidationError as exc:
        raise ImportContractError(f"invalid import page: {exc}") from exc
    records: list[ProviderRecord] = []
    for record in checked.records:
        try:
            records.append(ProviderRecord.model_validate(record.model_dump()))
        except ValidationError as exc:
            raise ImportContractError(f"invalid provider record: {exc}") from exc
    return checked.model_copy(update={"records": tuple(records)})


def _validated_barrier(value: object) -> str:
    raw = _runtime_value(value)
    if (
        not isinstance(raw, str)
        or not 1 <= len(raw) <= MAX_BARRIER_LENGTH
        or re.fullmatch(_NAME_PATTERN, raw) is None
    ):
        raise ImportContractError("barrier must be a bounded exact name")
    return raw


def _validated_reason(value: object) -> QuarantineReason:
    raw = _runtime_value(value)
    if raw not in ("failed", "cancelled", "poison", "superseded"):
        raise ImportContractError("quarantine reason is not supported")
    return raw


def _validated_source_key(flow: object, source: object) -> tuple[str, str]:
    identity = _validated_identity(
        ImportIdentity(
            flow=cast("str", flow),
            source=cast("str", source),
            run_epoch=1,
        )
    )
    return identity.flow, identity.source


def _record_canonical_bytes(record: ProviderRecord) -> bytes:
    payload = {
        "content": record.content,
        "record_id": record.record_id,
        "revision": record.revision,
    }
    try:
        document = json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise ImportContractError("provider record is not canonically serializable") from exc
    return b"adoption-import-record-v1\0" + document.encode("utf-8")


def _record_digest(record: ProviderRecord) -> str:
    return hashlib.sha256(_record_canonical_bytes(record)).hexdigest()


def _page_digest(ordinal_start: int, records: Sequence[ProviderRecord]) -> str:
    hasher = hashlib.sha256()
    hasher.update(b"adoption-import-page-v1\0")
    hasher.update(str(ordinal_start).encode("ascii"))
    hasher.update(b"\0")
    for offset, record in enumerate(records):
        hasher.update(str(ordinal_start + offset).encode("ascii"))
        hasher.update(b"\0")
        hasher.update(_record_digest(record).encode("ascii"))
        hasher.update(b"\0")
    return hasher.hexdigest()


def _chain_digest(previous: str, page_digest: str) -> str:
    hasher = hashlib.sha256()
    hasher.update(previous.encode("ascii"))
    hasher.update(b"\0")
    hasher.update(page_digest.encode("ascii"))
    return hasher.hexdigest()


def _digest(value: object, *, name: str) -> str:
    raw = _runtime_value(value)
    if not isinstance(raw, str) or _DIGEST_PATTERN.fullmatch(raw) is None:
        raise ImportStoreCorrupt(f"{name} is not a lowercase SHA-256 digest")
    return raw


def _row_text(value: object, *, name: str) -> str:
    raw = _runtime_value(value)
    if not isinstance(raw, str):
        raise ImportStoreCorrupt(f"persisted {name} is not text")
    return raw


def _row_optional_text(value: object, *, name: str) -> str | None:
    raw = _runtime_value(value)
    if raw is None:
        return None
    if not isinstance(raw, str):
        raise ImportStoreCorrupt(f"persisted {name} is not text or null")
    return raw


def _row_int(
    value: object,
    *,
    name: str,
    minimum: int = 0,
    maximum: int = MAX_ORDINAL,
) -> int:
    raw = _runtime_value(value)
    if isinstance(raw, bool) or type(raw) is not int:
        raise ImportStoreCorrupt(f"persisted {name} is not an exact int")
    if not minimum <= raw <= maximum:
        raise ImportStoreCorrupt(f"persisted {name} is outside its bound")
    return raw


def _row_revision(value: object, *, name: str = "revision") -> int:
    raw = _runtime_value(value)
    if isinstance(raw, bool):
        raise ImportStoreCorrupt(f"persisted {name} is not an exact integer")
    if isinstance(raw, int):
        result = raw
    elif isinstance(raw, Decimal):
        if not raw.is_finite() or raw != raw.to_integral_value():
            raise ImportStoreCorrupt(f"persisted {name} is not an exact integer")
        result = int(raw)
    elif isinstance(raw, str):
        try:
            result = int(raw, 10)
        except ValueError as exc:
            raise ImportStoreCorrupt(f"persisted {name} is not an exact integer") from exc
    else:
        raise ImportStoreCorrupt(f"persisted {name} is not an exact integer")
    if not 0 <= result <= MAX_RECORD_REVISION:
        raise ImportStoreCorrupt(f"persisted {name} is outside the provider revision bound")
    return result


def _row_time(value: object, *, name: str) -> datetime:
    raw = _runtime_value(value)
    if not isinstance(raw, datetime):
        raise ImportStoreCorrupt(f"persisted {name} is not a timestamp")
    return raw


def _row_optional_time(value: object, *, name: str) -> datetime | None:
    raw = _runtime_value(value)
    if raw is None:
        return None
    if not isinstance(raw, datetime):
        raise ImportStoreCorrupt(f"persisted {name} is not a timestamp or null")
    return raw


def _run_from_row(row: asyncpg.Record) -> _RunRow:
    state = _row_text(row["state"], name="run state")
    if state not in ("staging", "settled", "quarantined"):
        raise ImportStoreCorrupt("persisted run state is unknown")
    identity = ImportIdentity(
        flow=_row_text(row["flow"], name="flow"),
        source=_row_text(row["source"], name="source"),
        run_epoch=_row_int(row["run_epoch"], name="run_epoch", minimum=1, maximum=MAX_EPOCH),
    )
    return _RunRow(
        identity=identity,
        state=state,
        staged_pages=_row_int(row["staged_pages"], name="staged_pages"),
        staged_records=_row_int(row["staged_records"], name="staged_records"),
        staged_bytes=_row_int(row["staged_bytes"], name="staged_bytes"),
        staged_digest=_digest(row["staged_digest"], name="staged_digest"),
        staged_through_cursor=_row_optional_text(
            row["staged_through_cursor"], name="staged_through_cursor"
        ),
        staged_complete=bool(row["staged_complete"]),
        quarantine_reason=_row_optional_text(row["quarantine_reason"], name="quarantine_reason"),
        settled_barrier=_row_optional_text(row["settled_barrier"], name="settled_barrier"),
        settled_at=_row_optional_time(row["settled_at"], name="settled_at"),
        quarantined_at=_row_optional_time(row["quarantined_at"], name="quarantined_at"),
        created_at=_row_time(row["created_at"], name="created_at"),
        updated_at=_row_time(row["updated_at"], name="updated_at"),
    )


def _page_from_row(row: asyncpg.Record) -> _PageRow:
    return _PageRow(
        page_no=_row_int(row["page_no"], name="page_no", minimum=1),
        from_cursor=_row_optional_text(row["from_cursor"], name="from_cursor"),
        to_cursor=_row_optional_text(row["to_cursor"], name="to_cursor"),
        ordinal_start=_row_int(row["ordinal_start"], name="ordinal_start"),
        record_count=_row_int(row["record_count"], name="record_count"),
        payload_bytes=_row_int(row["payload_bytes"], name="page payload_bytes"),
        page_digest=_digest(row["page_digest"], name="page_digest"),
    )


def _record_from_row(row: asyncpg.Record) -> _RecordRow:
    record_id = _row_text(row["record_id"], name="record_id")
    content = _row_text(row["content"], name="content")
    if _RECORD_ID_PATTERN.fullmatch(record_id) is None:
        raise ImportCorruptStaging("persisted record_id is malformed")
    try:
        record = ProviderRecord(
            record_id=record_id,
            revision=_row_revision(row["revision"]),
            content=content,
        )
    except ValidationError as exc:
        raise ImportCorruptStaging("persisted record no longer satisfies ProviderRecord") from exc
    expected_bytes = len(_record_canonical_bytes(record))
    payload_bytes = _row_int(row["payload_bytes"], name="record payload_bytes")
    if payload_bytes != expected_bytes:
        raise ImportCorruptStaging("persisted record payload_bytes mismatch")
    expected_digest = _record_digest(record)
    stored_digest = _digest(row["record_digest"], name="record_digest")
    if stored_digest != expected_digest:
        raise ImportCorruptStaging("persisted record_digest mismatch")
    return _RecordRow(
        ordinal=_row_int(row["ordinal"], name="ordinal"),
        page_no=_row_int(row["page_no"], name="page_no", minimum=1),
        record=record,
        record_digest=stored_digest,
        payload_bytes=payload_bytes,
    )


def _settlement_from_row(row: asyncpg.Record) -> ImportSettlement:
    barrier = _row_text(row["barrier"], name="barrier")
    _validated_barrier(barrier)
    return ImportSettlement(
        identity=ImportIdentity(
            flow=_row_text(row["flow"], name="flow"),
            source=_row_text(row["source"], name="source"),
            run_epoch=_row_int(row["run_epoch"], name="run_epoch", minimum=1, maximum=MAX_EPOCH),
        ),
        barrier=barrier,
        page_count=_row_int(row["page_count"], name="page_count", minimum=1),
        record_count=_row_int(row["record_count"], name="record_count"),
        payload_bytes=_row_int(row["payload_bytes"], name="payload_bytes"),
        content_digest=_digest(row["content_digest"], name="content_digest"),
        upserted_count=_row_int(row["upserted_count"], name="upserted_count"),
        deleted_count=_row_int(row["deleted_count"], name="deleted_count"),
        history_count=_row_int(row["history_count"], name="history_count"),
        applied_through_ordinal=_row_int(
            row["applied_through_ordinal"], name="applied_through_ordinal", minimum=-1
        ),
        applied_cursor=_row_optional_text(row["applied_cursor"], name="applied_cursor"),
        settled_at=_row_time(row["settled_at"], name="settled_at"),
    )


def _progress(run: _RunRow) -> StageProgress:
    return StageProgress(
        identity=run.identity,
        staged_pages=run.staged_pages,
        staged_records=run.staged_records,
        staged_bytes=run.staged_bytes,
        staged_digest=run.staged_digest,
        staged_through_cursor=run.staged_through_cursor,
        complete=run.staged_complete,
    )


def _run_state(run: _RunRow) -> ImportRunState:
    return ImportRunState(
        identity=run.identity,
        state=run.state,
        staged_pages=run.staged_pages,
        staged_records=run.staged_records,
        staged_bytes=run.staged_bytes,
        staged_digest=run.staged_digest,
        staged_through_cursor=run.staged_through_cursor,
        staged_complete=run.staged_complete,
        quarantine_reason=run.quarantine_reason,
        settled_barrier=run.settled_barrier,
        settled_at=run.settled_at,
        quarantined_at=run.quarantined_at,
        created_at=run.created_at,
        updated_at=run.updated_at,
    )


def _check_transaction(conn: asyncpg.Connection) -> None:
    if not conn.is_in_transaction():
        raise ImportTransactionRequired(
            "staging/settlement require the caller's actual open transaction"
        )


class ImportStore:
    """Bounded application-owned staging/reconciliation store on one scratch schema.

    Construction performs no I/O. ``initialize(pool)`` is the explicit fixture
    bootstrap and is the only method that accepts a pool. Every other method
    accepts the caller's actual connection and never opens, commits, or rolls
    back a transaction. The caller owns transaction boundaries and connection
    release. Multiple handles for one scratch schema must use consistent finite
    bounds; bounds are validated on every staging/settlement call and are not a
    second durable authority.
    """

    def __init__(
        self,
        *,
        schema: str,
        max_page_records: int = DEFAULT_MAX_PAGE_RECORDS,
        max_page_bytes: int = DEFAULT_MAX_PAGE_BYTES,
        max_records: int = DEFAULT_MAX_RECORDS,
        max_total_bytes: int = DEFAULT_MAX_TOTAL_BYTES,
        max_pages: int = DEFAULT_MAX_PAGES,
    ) -> None:
        self._schema = _validated_schema(schema)
        self._max_page_records = _bounded_int(
            max_page_records,
            name="max_page_records",
            minimum=1,
            maximum=HARD_MAX_PAGE_RECORDS,
        )
        self._max_page_bytes = _bounded_int(
            max_page_bytes,
            name="max_page_bytes",
            minimum=1,
            maximum=HARD_MAX_PAGE_BYTES,
        )
        self._max_records = _bounded_int(
            max_records,
            name="max_records",
            minimum=1,
            maximum=HARD_MAX_RECORDS,
        )
        self._max_total_bytes = _bounded_int(
            max_total_bytes,
            name="max_total_bytes",
            minimum=1,
            maximum=HARD_MAX_TOTAL_BYTES,
        )
        self._max_pages = _bounded_int(
            max_pages,
            name="max_pages",
            minimum=1,
            maximum=HARD_MAX_PAGES,
        )
        if self._max_page_records > self._max_records:
            raise ImportContractError("max_page_records cannot exceed max_records")
        if self._max_page_bytes > self._max_total_bytes:
            raise ImportContractError("max_page_bytes cannot exceed max_total_bytes")
        self._t_current = f'"{self._schema}"."{_T_CURRENT}"'
        self._t_run = f'"{self._schema}"."{_T_RUN}"'
        self._t_page = f'"{self._schema}"."{_T_PAGE}"'
        self._t_record = f'"{self._schema}"."{_T_RECORD}"'
        self._t_mirror = f'"{self._schema}"."{_T_MIRROR}"'
        self._t_history = f'"{self._schema}"."{_T_HISTORY}"'
        self._t_watermark = f'"{self._schema}"."{_T_WATERMARK}"'
        self._t_settlement = f'"{self._schema}"."{_T_SETTLEMENT}"'

    @property
    def schema(self) -> str:
        return self._schema

    async def initialize(self, pool: asyncpg.Pool) -> None:
        """Create or validate only this fixture's scratch tables, idempotently.

        The explicit bootstrap takes one pool connection and one transaction to
        serialize short DDL under a schema-scoped advisory lock. A complete
        pre-existing shape with valid columns, keys, indexes, named CHECK
        constraints, and the history identity column is accepted without
        re-running DDL. Any incompatible pre-existing shape fails closed before
        application use; a failed bootstrap transaction leaves no partial new
        fixture shape. No staging or settlement method uses this pool afterward.
        """
        try:
            async with pool.acquire() as conn, conn.transaction():
                await conn.execute(
                    "SELECT pg_advisory_xact_lock(hashtext($1)::bigint)",
                    f"examples.adoption.import_store.initialize:{self._schema}",
                )
                await conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{self._schema}"')
                seen = await self._validate_shape(conn, existing_only=True)
                await self._validate_constraints(conn, existing_only=True)
                if seen == set(_EXPECTED_COLUMNS):
                    return
                for statement in self._ddl_statements():
                    await conn.execute(statement)
                await self._validate_shape(conn, existing_only=False)
                await self._validate_constraints(conn, existing_only=False)
        except ImportStoreError:
            raise
        except (asyncpg.PostgresError, asyncpg.InterfaceError) as exc:
            raise ImportSchemaError(
                f"import fixture bootstrap failed for schema {self._schema!r}: "
                f"{type(exc).__name__}: {exc}"
            ) from exc

    async def _validate_shape(self, conn: asyncpg.Connection, *, existing_only: bool) -> set[str]:
        rows = await conn.fetch(
            """
            SELECT table_name, column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = $1 AND table_name = ANY($2::text[])
            """,
            self._schema,
            list(_EXPECTED_COLUMNS),
        )
        present: dict[str, set[str]] = {table: set() for table in _EXPECTED_COLUMNS}
        types: dict[str, dict[str, str]] = {table: {} for table in _EXPECTED_COLUMNS}
        seen: set[str] = set()
        for row in rows:
            table = row["table_name"]
            column = row["column_name"]
            data_type = row["data_type"]
            if isinstance(table, str) and table in present:
                seen.add(table)
                if isinstance(column, str):
                    present[table].add(column)
                    if isinstance(data_type, str):
                        types[table][column] = data_type
        missing = [
            f"{table}.{column}"
            for table in sorted(_EXPECTED_COLUMNS)
            if (not existing_only or table in seen)
            for column in sorted(_EXPECTED_COLUMNS[table] - present[table])
        ]
        if missing:
            shown = ", ".join(missing[:12])
            raise ImportSchemaError(
                f"scratch schema {self._schema!r} has incompatible pre-existing tables "
                f"(missing: {shown}); use a fresh scratch schema or drop this fixture's tables"
            )
        wrong_types = [
            f"{table}.{column} expected {expected} got {types[table].get(column, 'unknown')}"
            for table in sorted(_EXPECTED_COLUMNS)
            if (not existing_only or table in seen)
            for column, expected in sorted(_EXPECTED_TYPES[table].items())
            if types[table].get(column) != expected
        ]
        if wrong_types:
            shown = ", ".join(wrong_types[:12])
            raise ImportSchemaError(
                f"scratch schema {self._schema!r} has incompatible pre-existing column "
                f"types: {shown}; use a fresh scratch schema."
            )
        return seen

    async def _validate_constraints(self, conn: asyncpg.Connection, *, existing_only: bool) -> None:
        table_rows = await conn.fetch(
            """
            SELECT c.oid::bigint AS table_oid, c.relname
            FROM pg_class AS c
            JOIN pg_namespace AS n ON n.oid = c.relnamespace
            WHERE n.nspname = $1 AND c.relkind = 'r'
              AND c.relname = ANY($2::text[])
            """,
            self._schema,
            list(_EXPECTED_COLUMNS),
        )
        oid_by_table: dict[str, int] = {}
        for row in table_rows:
            table = _schema_text(row["relname"], name="table name")
            if table in _EXPECTED_COLUMNS:
                oid_by_table[table] = _schema_int(row["table_oid"], name="table oid")
        active_tables = [
            table for table in _EXPECTED_COLUMNS if not existing_only or table in oid_by_table
        ]
        missing_tables = [table for table in active_tables if table not in oid_by_table]
        if missing_tables:
            shown = ", ".join(sorted(missing_tables))
            raise ImportSchemaError(
                f"scratch schema {self._schema!r} is missing fixture table(s): {shown}"
            )
        if not oid_by_table:
            return
        oids = list(oid_by_table.values())
        oid_to_table = {oid: table for table, oid in oid_by_table.items()}
        attribute_rows = await conn.fetch(
            """
            SELECT attrelid::bigint AS table_oid, attnum, attname
            FROM pg_attribute
            WHERE attrelid = ANY($1::oid[]) AND attnum > 0 AND NOT attisdropped
            """,
            oids,
        )
        attributes: dict[tuple[int, int], str] = {}
        for row in attribute_rows:
            table_oid = _schema_int(row["table_oid"], name="attribute relation oid")
            attnum = _schema_int(row["attnum"], name="attribute number")
            attributes[(table_oid, attnum)] = _schema_text(row["attname"], name="attribute name")

        constraint_rows = await conn.fetch(
            """
            SELECT
                c.conrelid::bigint AS table_oid,
                c.conname,
                c.contype::text AS contype,
                c.conkey,
                c.condeferrable,
                c.convalidated,
                i.indisunique,
                i.indisvalid,
                i.indisready,
                (i.indpred IS NULL) AS index_is_plain
            FROM pg_constraint AS c
            LEFT JOIN pg_index AS i ON i.indexrelid = c.conindid
            WHERE c.conrelid = ANY($1::oid[]) AND c.contype IN ('p', 'u', 'c')
            ORDER BY c.conrelid, c.conname
            """,
            oids,
        )
        keys_by_table: dict[str, list[_KeySpec]] = {}
        checks_by_table: dict[str, dict[str, bool]] = {}
        invalid_keys: list[str] = []
        for row in constraint_rows:
            table_oid = _schema_int(row["table_oid"], name="constraint relation oid")
            table = oid_to_table.get(table_oid)
            if table is None:
                raise ImportSchemaError("catalog constraint references an unknown table")
            name = _schema_text(row["conname"], name="constraint name")
            constraint_type = _schema_text(row["contype"], name="constraint type")
            if constraint_type in ("p", "u"):
                key_columns: list[str] = []
                for attnum in _schema_attnums(row["conkey"]):
                    column = attributes.get((table_oid, attnum))
                    if column is None:
                        raise ImportSchemaError(
                            f"catalog key {table}.{name} references a missing column"
                        )
                    key_columns.append(column)
                deferrable = _schema_bool(row["condeferrable"], name="condeferrable")
                validated = _schema_bool(row["convalidated"], name="convalidated")
                index_unique = _schema_bool(row["indisunique"], name="indisunique")
                index_valid = _schema_bool(row["indisvalid"], name="indisvalid")
                index_ready = _schema_bool(row["indisready"], name="indisready")
                index_plain = _schema_bool(row["index_is_plain"], name="index_is_plain")
                if (
                    deferrable
                    or not validated
                    or not index_unique
                    or not index_valid
                    or not index_ready
                    or not index_plain
                ):
                    invalid_keys.append(f"{table}.{name}")
                keys_by_table.setdefault(table, []).append((constraint_type, tuple(key_columns)))
            elif constraint_type == "c":
                checks_by_table.setdefault(table, {})[name] = _schema_bool(
                    row["convalidated"], name="check convalidated"
                )
        if invalid_keys:
            shown = ", ".join(sorted(invalid_keys)[:12])
            raise ImportSchemaError(
                f"scratch schema {self._schema!r} has non-immediate, unvalidated, "
                f"or non-unique key/index identity: {shown}"
            )
        for table, expected_keys in _KEY_CONSTRAINTS.items():
            if existing_only and table not in oid_by_table:
                continue
            observed: list[_KeySpec] = keys_by_table.get(table, [])
            for kind, columns in expected_keys:
                if (kind, columns) not in observed:
                    label = "primary" if kind == "p" else "unique"
                    raise ImportSchemaError(
                        f"scratch schema {self._schema!r} is missing the {label} "
                        f"key {table}{columns}"
                    )
        for table, expected_checks in _CHECK_CONSTRAINTS.items():
            if existing_only and table not in oid_by_table:
                continue
            observed_checks = checks_by_table.get(table, {})
            missing_checks = sorted(expected_checks - observed_checks.keys())
            if missing_checks:
                shown = ", ".join(missing_checks[:12])
                raise ImportSchemaError(
                    f"scratch schema {self._schema!r} is missing named CHECK "
                    f"constraint(s) on {table}: {shown}"
                )
            unvalidated_checks = sorted(
                name for name in expected_checks if not observed_checks[name]
            )
            if unvalidated_checks:
                shown = ", ".join(unvalidated_checks[:12])
                raise ImportSchemaError(
                    f"scratch schema {self._schema!r} has unvalidated CHECK "
                    f"constraint(s) on {table}: {shown}"
                )
        if _T_SETTLEMENT in oid_by_table:
            await self._probe_history_sum_check(conn)
        history_oid = oid_by_table.get(_T_HISTORY)
        if history_oid is not None:
            identity = await conn.fetchval(
                """
                SELECT attidentity::text AS attidentity
                FROM pg_attribute
                WHERE attrelid = $1 AND attname = 'seq' AND NOT attisdropped
                """,
                history_oid,
            )
            if _schema_text(identity, name="history.seq identity") != "a":
                raise ImportSchemaError(
                    f"scratch schema {self._schema!r} lost the history identity column"
                )

    async def _probe_history_sum_check(self, conn: asyncpg.Connection) -> None:
        """Prove the named history-sum CHECK accepts the valid tuple and rejects a mismatch.

        This is a savepoint-scoped semantic probe, not SQL-text comparison:
        the real PostgreSQL constraint must reject ``history_count=1`` for
        ``upserted_count=deleted_count=0`` with the named CHECK and accept the
        all-zero tuple. Both writes are rolled back before bootstrap continues.
        """
        probe_flow = "__shape_probe_" + secrets.token_hex(8)
        probe_source = "__shape_probe_" + secrets.token_hex(8)
        digest = "0" * 64
        await conn.execute("SAVEPOINT import_store_history_sum_probe")
        try:
            try:
                await conn.execute(
                    f"""
                    INSERT INTO {self._t_settlement}
                        (flow, source, run_epoch, barrier, page_count, record_count,
                         payload_bytes, content_digest, upserted_count, deleted_count,
                         history_count, applied_through_ordinal, applied_cursor)
                    VALUES ($1, $2, 1, 'invalid', 1, 0, 0, $3, 0, 0, 1, -1, NULL)
                    """,
                    probe_flow,
                    probe_source,
                    digest,
                )
            except asyncpg.CheckViolationError as exc:
                if exc.constraint_name != "adoption_import_settlement_history_sum_check":
                    raise ImportSchemaError(
                        f"scratch schema {self._schema!r} rejected the history-sum probe "
                        f"with a different constraint: {exc.constraint_name!r}"
                    ) from exc
            else:
                raise ImportSchemaError(
                    f"scratch schema {self._schema!r} does not enforce the named "
                    "history_count = upserted_count + deleted_count CHECK"
                )
            await conn.execute("ROLLBACK TO SAVEPOINT import_store_history_sum_probe")
            try:
                await conn.execute(
                    f"""
                    INSERT INTO {self._t_settlement}
                        (flow, source, run_epoch, barrier, page_count, record_count,
                         payload_bytes, content_digest, upserted_count, deleted_count,
                         history_count, applied_through_ordinal, applied_cursor)
                    VALUES ($1, $2, 1, 'valid', 1, 0, 0, $3, 0, 0, 0, -1, NULL)
                    """,
                    probe_flow,
                    probe_source,
                    digest,
                )
            except (asyncpg.PostgresError, asyncpg.InterfaceError) as exc:
                raise ImportSchemaError(
                    f"scratch schema {self._schema!r} rejects a valid history-sum tuple: "
                    f"{type(exc).__name__}: {exc}"
                ) from exc
            await conn.execute("ROLLBACK TO SAVEPOINT import_store_history_sum_probe")
        except BaseException:
            with contextlib.suppress(asyncpg.PostgresError, asyncpg.InterfaceError):
                await conn.execute("ROLLBACK TO SAVEPOINT import_store_history_sum_probe")
            raise
        finally:
            with contextlib.suppress(asyncpg.PostgresError, asyncpg.InterfaceError):
                await conn.execute("RELEASE SAVEPOINT import_store_history_sum_probe")

    def _ddl_statements(self) -> tuple[str, ...]:
        schema = f'"{self._schema}"'
        return (
            f"""
            CREATE TABLE IF NOT EXISTS {schema}."{_T_CURRENT}" (
                flow text NOT NULL,
                source text NOT NULL,
                current_epoch bigint NOT NULL,
                updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
                CONSTRAINT adoption_import_current_pkey PRIMARY KEY (flow, source),
                CONSTRAINT adoption_import_current_flow_len_check
                    CHECK (length(flow) BETWEEN 1 AND {MAX_FLOW_LENGTH}),
                CONSTRAINT adoption_import_current_source_len_check
                    CHECK (length(source) BETWEEN 1 AND {MAX_SOURCE_LENGTH}),
                CONSTRAINT adoption_import_current_epoch_check CHECK (current_epoch >= 1)
            )
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {schema}."{_T_RUN}" (
                flow text NOT NULL,
                source text NOT NULL,
                run_epoch bigint NOT NULL,
                state text NOT NULL,
                staged_pages bigint NOT NULL DEFAULT 0,
                staged_records bigint NOT NULL DEFAULT 0,
                staged_bytes bigint NOT NULL DEFAULT 0,
                staged_digest text NOT NULL,
                staged_through_cursor text,
                staged_complete boolean NOT NULL DEFAULT false,
                quarantine_reason text,
                created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
                updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
                settled_barrier text,
                settled_at timestamptz,
                quarantined_at timestamptz,
                CONSTRAINT adoption_import_run_pkey PRIMARY KEY (flow, source, run_epoch),
                CONSTRAINT adoption_import_run_epoch_check CHECK (run_epoch >= 1),
                CONSTRAINT adoption_import_run_state_check
                    CHECK (state IN ('staging', 'settled', 'quarantined')),
                CONSTRAINT adoption_import_run_staged_pages_check CHECK (staged_pages >= 0),
                CONSTRAINT adoption_import_run_staged_records_check CHECK (staged_records >= 0),
                CONSTRAINT adoption_import_run_staged_bytes_check CHECK (staged_bytes >= 0),
                CONSTRAINT adoption_import_run_digest_check
                    CHECK (staged_digest ~ '^[0-9a-f]{{64}}$'),
                CONSTRAINT adoption_import_run_quarantine_reason_check
                    CHECK (state <> 'quarantined' OR quarantine_reason IS NOT NULL),
                CONSTRAINT adoption_import_run_settled_marker_check
                    CHECK (
                        state <> 'settled'
                        OR (settled_barrier IS NOT NULL AND settled_at IS NOT NULL)
                    )
            )
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {schema}."{_T_PAGE}" (
                flow text NOT NULL,
                source text NOT NULL,
                run_epoch bigint NOT NULL,
                page_no bigint NOT NULL,
                from_cursor text,
                to_cursor text,
                ordinal_start bigint NOT NULL,
                record_count bigint NOT NULL,
                payload_bytes bigint NOT NULL,
                page_digest text NOT NULL,
                staged_at timestamptz NOT NULL DEFAULT clock_timestamp(),
                CONSTRAINT adoption_import_page_pkey
                    PRIMARY KEY (flow, source, run_epoch, page_no),
                CONSTRAINT adoption_import_page_ordinal_key
                    UNIQUE (flow, source, run_epoch, ordinal_start),
                CONSTRAINT adoption_import_page_epoch_check CHECK (run_epoch >= 1),
                CONSTRAINT adoption_import_page_no_check CHECK (page_no >= 1),
                CONSTRAINT adoption_import_page_ordinal_check CHECK (ordinal_start >= 0),
                CONSTRAINT adoption_import_page_record_count_check CHECK (record_count >= 0),
                CONSTRAINT adoption_import_page_payload_check CHECK (payload_bytes >= 0),
                CONSTRAINT adoption_import_page_digest_check
                    CHECK (page_digest ~ '^[0-9a-f]{{64}}$'),
                CONSTRAINT adoption_import_page_cursor_progress_check
                    CHECK (to_cursor IS NULL OR to_cursor <> from_cursor),
                CONSTRAINT adoption_import_page_nonempty_check
                    CHECK (to_cursor IS NULL OR record_count > 0)
            )
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {schema}."{_T_RECORD}" (
                flow text NOT NULL,
                source text NOT NULL,
                run_epoch bigint NOT NULL,
                ordinal bigint NOT NULL,
                page_no bigint NOT NULL,
                record_id text NOT NULL,
                revision numeric(20, 0) NOT NULL,
                content text NOT NULL,
                record_digest text NOT NULL,
                payload_bytes bigint NOT NULL,
                CONSTRAINT adoption_import_record_pkey
                    PRIMARY KEY (flow, source, run_epoch, ordinal),
                CONSTRAINT adoption_import_record_id_key
                    UNIQUE (flow, source, run_epoch, record_id),
                CONSTRAINT adoption_import_record_page_key
                    UNIQUE (flow, source, run_epoch, page_no, ordinal),
                CONSTRAINT adoption_import_record_epoch_check CHECK (run_epoch >= 1),
                CONSTRAINT adoption_import_record_ordinal_check CHECK (ordinal >= 0),
                CONSTRAINT adoption_import_record_page_no_check CHECK (page_no >= 1),
                CONSTRAINT adoption_import_record_id_len_check
                    CHECK (length(record_id) BETWEEN 1 AND {MAX_RECORD_ID_LENGTH}),
                CONSTRAINT adoption_import_record_revision_check
                    CHECK (revision >= 0 AND revision <= {MAX_RECORD_REVISION}),
                CONSTRAINT adoption_import_record_content_check
                    CHECK (octet_length(content) <= {MAX_RECORD_CONTENT_BYTES}),
                CONSTRAINT adoption_import_record_digest_check
                    CHECK (record_digest ~ '^[0-9a-f]{{64}}$'),
                CONSTRAINT adoption_import_record_payload_check CHECK (payload_bytes >= 0)
            )
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {schema}."{_T_MIRROR}" (
                flow text NOT NULL,
                source text NOT NULL,
                record_id text NOT NULL,
                revision numeric(20, 0) NOT NULL,
                content text NOT NULL,
                applied_epoch bigint NOT NULL,
                updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
                CONSTRAINT adoption_import_mirror_pkey PRIMARY KEY (flow, source, record_id),
                CONSTRAINT adoption_import_mirror_id_len_check
                    CHECK (length(record_id) BETWEEN 1 AND {MAX_RECORD_ID_LENGTH}),
                CONSTRAINT adoption_import_mirror_revision_check
                    CHECK (revision >= 0 AND revision <= {MAX_RECORD_REVISION}),
                CONSTRAINT adoption_import_mirror_content_check
                    CHECK (octet_length(content) <= {MAX_RECORD_CONTENT_BYTES}),
                CONSTRAINT adoption_import_mirror_epoch_check CHECK (applied_epoch >= 1)
            )
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {schema}."{_T_HISTORY}" (
                seq bigint GENERATED ALWAYS AS IDENTITY,
                flow text NOT NULL,
                source text NOT NULL,
                run_epoch bigint NOT NULL,
                barrier text NOT NULL,
                kind text NOT NULL,
                record_id text NOT NULL,
                ordinal bigint,
                revision numeric(20, 0) NOT NULL,
                content text NOT NULL,
                recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
                CONSTRAINT adoption_import_history_pkey PRIMARY KEY (seq),
                CONSTRAINT adoption_import_history_identity_key
                    UNIQUE (flow, source, run_epoch, barrier, kind, record_id),
                CONSTRAINT adoption_import_history_epoch_check CHECK (run_epoch >= 1),
                CONSTRAINT adoption_import_history_kind_check
                    CHECK (kind IN ('upsert', 'delete')),
                CONSTRAINT adoption_import_history_revision_check
                    CHECK (revision >= 0 AND revision <= {MAX_RECORD_REVISION}),
                CONSTRAINT adoption_import_history_ordinal_check
                    CHECK (
                        (kind = 'upsert' AND ordinal IS NOT NULL AND ordinal >= 0)
                        OR (kind = 'delete' AND ordinal IS NULL)
                    )
            )
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {schema}."{_T_WATERMARK}" (
                flow text NOT NULL,
                source text NOT NULL,
                applied_epoch bigint NOT NULL,
                applied_through_ordinal bigint NOT NULL,
                applied_cursor text,
                updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
                CONSTRAINT adoption_import_watermark_pkey PRIMARY KEY (flow, source),
                CONSTRAINT adoption_import_watermark_epoch_check CHECK (applied_epoch >= 1),
                CONSTRAINT adoption_import_watermark_ordinal_check
                    CHECK (applied_through_ordinal >= -1)
            )
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {schema}."{_T_SETTLEMENT}" (
                flow text NOT NULL,
                source text NOT NULL,
                run_epoch bigint NOT NULL,
                barrier text NOT NULL,
                page_count bigint NOT NULL,
                record_count bigint NOT NULL,
                payload_bytes bigint NOT NULL,
                content_digest text NOT NULL,
                upserted_count bigint NOT NULL,
                deleted_count bigint NOT NULL,
                history_count bigint NOT NULL,
                applied_through_ordinal bigint NOT NULL,
                applied_cursor text,
                settled_at timestamptz NOT NULL DEFAULT clock_timestamp(),
                CONSTRAINT adoption_import_settlement_pkey
                    PRIMARY KEY (flow, source, run_epoch, barrier),
                CONSTRAINT adoption_import_settlement_run_key
                    UNIQUE (flow, source, run_epoch),
                CONSTRAINT adoption_import_settlement_epoch_check CHECK (run_epoch >= 1),
                CONSTRAINT adoption_import_settlement_page_count_check CHECK (page_count >= 1),
                CONSTRAINT adoption_import_settlement_record_count_check CHECK (record_count >= 0),
                CONSTRAINT adoption_import_settlement_payload_check CHECK (payload_bytes >= 0),
                CONSTRAINT adoption_import_settlement_digest_check
                    CHECK (content_digest ~ '^[0-9a-f]{{64}}$'),
                CONSTRAINT adoption_import_settlement_upserted_check CHECK (upserted_count >= 0),
                CONSTRAINT adoption_import_settlement_deleted_check CHECK (deleted_count >= 0),
                CONSTRAINT adoption_import_settlement_history_count_check
                    CHECK (history_count >= 0),
                CONSTRAINT adoption_import_settlement_applied_ordinal_check
                    CHECK (applied_through_ordinal >= -1),
                CONSTRAINT adoption_import_settlement_history_sum_check
                    CHECK (history_count = upserted_count + deleted_count)
            )
            """,
        )

    async def open_source_run(
        self,
        conn: asyncpg.Connection,
        identity: ImportIdentity,
        *,
        expected_current_epoch: int | None,
    ) -> ImportRunState:
        """Open the next exact epoch after an explicit current-epoch CAS.

        The first epoch must be ``1``. A later epoch must be exactly
        ``current + 1`` and the previous current run must be settled or
        quarantined. The pointer update and run insert are in the caller's
        transaction; there is no hidden commit.
        """
        _check_transaction(conn)
        checked = _validated_identity(identity)
        if expected_current_epoch is not None:
            expected_current_epoch = _bounded_int(
                expected_current_epoch,
                name="expected_current_epoch",
                minimum=1,
                maximum=MAX_EPOCH,
            )
        await conn.execute(
            "SELECT pg_advisory_xact_lock(hashtext($1)::bigint)",
            f"examples.adoption.import_store.source:{self._schema}:{checked.flow}:{checked.source}",
        )
        current_row = await conn.fetchrow(
            f"SELECT current_epoch FROM {self._t_current} "
            "WHERE flow = $1 AND source = $2 FOR UPDATE",
            checked.flow,
            checked.source,
        )
        if current_row is None:
            if expected_current_epoch is not None or checked.run_epoch != 1:
                raise ImportEpochCASFailed(
                    f"first epoch for {checked.flow!r}/{checked.source!r} must be 1 "
                    f"with expected_current_epoch=None"
                )
            await conn.execute(
                f"INSERT INTO {self._t_current} (flow, source, current_epoch) VALUES ($1, $2, $3)",
                checked.flow,
                checked.source,
                checked.run_epoch,
            )
        else:
            current_epoch = _row_int(
                current_row["current_epoch"],
                name="current_epoch",
                minimum=1,
                maximum=MAX_EPOCH,
            )
            if expected_current_epoch != current_epoch or checked.run_epoch != current_epoch + 1:
                raise ImportEpochCASFailed(
                    f"epoch CAS failed: expected {expected_current_epoch!r}, "
                    f"current is {current_epoch}, requested {checked.run_epoch}"
                )
            prior_row = await conn.fetchrow(
                f"SELECT state FROM {self._t_run} "
                "WHERE flow = $1 AND source = $2 AND run_epoch = $3 FOR UPDATE",
                checked.flow,
                checked.source,
                current_epoch,
            )
            if prior_row is None:
                raise ImportStoreCorrupt("current epoch has no persisted run row")
            prior_state = _row_text(prior_row["state"], name="prior run state")
            if prior_state not in ("settled", "quarantined"):
                raise ImportRunActive(
                    f"previous epoch {current_epoch} is still {prior_state!r}; "
                    "quarantine or settle it before opening a newer epoch"
                )
            await conn.execute(
                f"UPDATE {self._t_current} SET current_epoch = $3, "
                "updated_at = clock_timestamp() WHERE flow = $1 AND source = $2",
                checked.flow,
                checked.source,
                checked.run_epoch,
            )
        try:
            row = await conn.fetchrow(
                f"""
                INSERT INTO {self._t_run}
                    (flow, source, run_epoch, state, staged_digest)
                VALUES ($1, $2, $3, 'staging', $4)
                RETURNING *
                """,
                checked.flow,
                checked.source,
                checked.run_epoch,
                _EMPTY_RUN_DIGEST,
            )
        except asyncpg.UniqueViolationError as exc:
            raise ImportEpochCASFailed(
                f"run epoch {checked.run_epoch} already exists for "
                f"{checked.flow!r}/{checked.source!r}"
            ) from exc
        if row is None:
            raise ImportStoreCorrupt("run insert returned no row")
        return _run_state(_run_from_row(row))

    async def stage_page(
        self,
        conn: asyncpg.Connection,
        identity: ImportIdentity,
        page: ImportPage,
    ) -> StageProgress:
        """Atomically append one exact page and advance staging in *this* transaction.

        The caller must already be in the managed transaction. An exact replay
        of an already staged page is a no-op; a divergent replay, cursor jump,
        ordinal gap, sealed source, stale epoch, or bound breach raises a typed
        error before any write in that statement group.
        """
        _check_transaction(conn)
        checked_identity = _validated_identity(identity)
        checked_page = _validated_page(page)
        run = await self._lock_run_for_write(conn, checked_identity)
        if run.state == "settled":
            raise ImportAlreadySettled("staged run is already settled")
        if run.state == "quarantined":
            raise ImportRunNotOpen("quarantined staging is inert and cannot advance")

        prepared: list[tuple[ProviderRecord, str, int]] = []
        for record in checked_page.records:
            digest = _record_digest(record)
            prepared.append((record, digest, len(_record_canonical_bytes(record))))
        page_bytes = sum(item[2] for item in prepared)
        page_digest = _page_digest(checked_page.ordinal_start, checked_page.records)
        if len(prepared) > self._max_page_records:
            raise ImportBoundExceeded("page_records", self._max_page_records, len(prepared))
        if page_bytes > self._max_page_bytes:
            raise ImportBoundExceeded("page_bytes", self._max_page_bytes, page_bytes)

        existing_row = await conn.fetchrow(
            f"SELECT * FROM {self._t_page} "
            "WHERE flow = $1 AND source = $2 AND run_epoch = $3 AND ordinal_start = $4",
            checked_identity.flow,
            checked_identity.source,
            checked_identity.run_epoch,
            checked_page.ordinal_start,
        )
        if existing_row is not None:
            existing = _page_from_row(existing_row)
            if not (
                existing.record_count == len(prepared)
                and existing.payload_bytes == page_bytes
                and existing.page_digest == page_digest
                and existing.from_cursor == checked_page.cursor
                and existing.to_cursor == checked_page.next_cursor
            ):
                raise ImportStagingDiverged(
                    f"page replay at ordinal {checked_page.ordinal_start} diverged"
                )
            return _progress(run)

        if run.staged_complete:
            raise ImportRunNotOpen("source is already sealed; no further page is stageable")
        if checked_page.ordinal_start != run.staged_records:
            if checked_page.ordinal_start < run.staged_records:
                raise ImportStagingDiverged(
                    f"page starts inside staged content at ordinal {checked_page.ordinal_start}"
                )
            raise ImportOrdinalGap(run.staged_records, checked_page.ordinal_start)
        if checked_page.cursor != run.staged_through_cursor:
            raise ImportCursorMismatch(run.staged_through_cursor, checked_page.cursor)
        if run.staged_pages + 1 > self._max_pages:
            raise ImportBoundExceeded("pages", self._max_pages, run.staged_pages + 1)
        if run.staged_records + len(prepared) > self._max_records:
            raise ImportBoundExceeded(
                "records", self._max_records, run.staged_records + len(prepared)
            )
        if run.staged_bytes + page_bytes > self._max_total_bytes:
            raise ImportBoundExceeded(
                "total_bytes", self._max_total_bytes, run.staged_bytes + page_bytes
            )

        page_no = run.staged_pages + 1
        record_params = [
            (
                checked_identity.flow,
                checked_identity.source,
                checked_identity.run_epoch,
                checked_page.ordinal_start + offset,
                page_no,
                record.record_id,
                Decimal(record.revision),
                record.content,
                digest,
                payload_bytes,
            )
            for offset, (record, digest, payload_bytes) in enumerate(prepared)
        ]
        if record_params:
            try:
                await conn.executemany(
                    f"""
                    INSERT INTO {self._t_record}
                        (flow, source, run_epoch, ordinal, page_no, record_id,
                         revision, content, record_digest, payload_bytes)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                    """,
                    record_params,
                )
            except asyncpg.UniqueViolationError as exc:
                raise ImportStagingDiverged(
                    "a record id was already staged in this run; duplicate content "
                    "across pages is not a new revision"
                ) from exc
        await conn.execute(
            f"""
            INSERT INTO {self._t_page}
                (flow, source, run_epoch, page_no, from_cursor, to_cursor,
                 ordinal_start, record_count, payload_bytes, page_digest)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            """,
            checked_identity.flow,
            checked_identity.source,
            checked_identity.run_epoch,
            page_no,
            checked_page.cursor,
            checked_page.next_cursor,
            checked_page.ordinal_start,
            len(prepared),
            page_bytes,
            page_digest,
        )
        new_digest = _chain_digest(run.staged_digest, page_digest)
        updated = await conn.fetchval(
            f"""
            UPDATE {self._t_run}
            SET staged_pages = $4,
                staged_records = $5,
                staged_bytes = $6,
                staged_digest = $7,
                staged_through_cursor = $8,
                staged_complete = $9,
                updated_at = clock_timestamp()
            WHERE flow = $1 AND source = $2 AND run_epoch = $3
              AND state = 'staging' AND staged_pages = $10
            RETURNING 1
            """,
            checked_identity.flow,
            checked_identity.source,
            checked_identity.run_epoch,
            page_no,
            run.staged_records + len(prepared),
            run.staged_bytes + page_bytes,
            new_digest,
            checked_page.next_cursor,
            not checked_page.has_more,
            run.staged_pages,
        )
        if updated != 1:
            raise ImportStoreCorrupt("staging advance did not update exactly one run row")
        return StageProgress(
            identity=checked_identity,
            staged_pages=page_no,
            staged_records=run.staged_records + len(prepared),
            staged_bytes=run.staged_bytes + page_bytes,
            staged_digest=new_digest,
            staged_through_cursor=checked_page.next_cursor,
            complete=not checked_page.has_more,
        )

    async def quarantine(
        self,
        conn: asyncpg.Connection,
        identity: ImportIdentity,
        *,
        reason: QuarantineReason,
    ) -> ImportRunState:
        """Mark a run inert without deleting its bounded retained staging rows.

        Quarantine never changes the domain mirror/history/watermark and never
        makes staging consumable by another run. An already quarantined run is
        an idempotent no-op.
        """
        _check_transaction(conn)
        checked_identity = _validated_identity(identity)
        checked_reason = _validated_reason(reason)
        # Quarantine is deliberately allowed for an old epoch: it locks the
        # source pointer for serialization but does not claim current
        # ownership, so late cleanup can only mark the old run inert.
        await self._lock_current_any(conn, checked_identity)
        row = await conn.fetchrow(
            f"SELECT * FROM {self._t_run} "
            "WHERE flow = $1 AND source = $2 AND run_epoch = $3 FOR UPDATE",
            checked_identity.flow,
            checked_identity.source,
            checked_identity.run_epoch,
        )
        if row is None:
            raise ImportRunMissing("cannot quarantine a missing run")
        run = _run_from_row(row)
        if run.state == "settled":
            raise ImportAlreadySettled("a settled run cannot be quarantined")
        if run.state == "quarantined":
            return _run_state(run)
        updated = await conn.fetchrow(
            f"""
            UPDATE {self._t_run}
            SET state = 'quarantined', quarantine_reason = $4,
                quarantined_at = clock_timestamp(), updated_at = clock_timestamp()
            WHERE flow = $1 AND source = $2 AND run_epoch = $3 AND state = 'staging'
            RETURNING *
            """,
            checked_identity.flow,
            checked_identity.source,
            checked_identity.run_epoch,
            checked_reason,
        )
        if updated is None:
            raise ImportStoreCorrupt("quarantine did not update exactly one staging run")
        return _run_state(_run_from_row(updated))

    async def commit_reconciliation(
        self,
        conn: asyncpg.Connection,
        identity: ImportIdentity,
        *,
        barrier: str,
        max_deletions: int,
    ) -> CommitResult:
        """Apply one complete staged dataset in the caller's one transaction.

        Dataset/epoch/ownership, exact page/ordinal/cursor/content-digest,
        source-statistics, finite-bound, and anti-join deletion-bound
        validations happen before any domain write. A later failure (for
        example a corrupted watermark tie) raises inside the caller's unit, so
        the caller's transaction must roll back the whole application unit.
        The method itself never commits. Replaying the same settlement identity
        returns its existing marker without touching history, mirror,
        watermark, or a newer epoch.
        """
        _check_transaction(conn)
        checked_identity = _validated_identity(identity)
        checked_barrier = _validated_barrier(barrier)
        checked_deletions = _bounded_int(
            max_deletions,
            name="max_deletions",
            minimum=0,
            maximum=HARD_MAX_DELETIONS,
        )
        await self._lock_current(conn, checked_identity)
        row = await conn.fetchrow(
            f"SELECT * FROM {self._t_run} "
            "WHERE flow = $1 AND source = $2 AND run_epoch = $3 FOR UPDATE",
            checked_identity.flow,
            checked_identity.source,
            checked_identity.run_epoch,
        )
        if row is None:
            raise ImportRunMissing("cannot settle a missing run")
        run = _run_from_row(row)
        if run.state == "settled":
            if run.settled_barrier != checked_barrier:
                raise ImportAlreadySettled(
                    f"run is already settled under barrier {run.settled_barrier!r}"
                )
            settlement_row = await conn.fetchrow(
                f"SELECT * FROM {self._t_settlement} "
                "WHERE flow = $1 AND source = $2 AND run_epoch = $3 AND barrier = $4",
                checked_identity.flow,
                checked_identity.source,
                checked_identity.run_epoch,
                checked_barrier,
            )
            if settlement_row is None:
                raise ImportStoreCorrupt("settled run is missing its settlement marker")
            return CommitResult(settlement=_settlement_from_row(settlement_row), replayed=True)
        if run.state == "quarantined":
            raise ImportRunNotOpen("quarantined staging cannot be settled")
        if not run.staged_complete:
            raise ImportSourceIncomplete("source has not staged its terminal page")
        settlement_row = await conn.fetchrow(
            f"SELECT * FROM {self._t_settlement} "
            "WHERE flow = $1 AND source = $2 AND run_epoch = $3",
            checked_identity.flow,
            checked_identity.source,
            checked_identity.run_epoch,
        )
        if settlement_row is not None:
            raise ImportStoreCorrupt("staging run already has a settlement marker")

        dataset = await self._validate_dataset(conn, run)
        candidates = await self._deletion_candidates(conn, checked_identity)
        if len(candidates) > checked_deletions:
            raise ImportMassDeleteRefused(len(candidates), checked_deletions)

        upsert_params = [
            (
                checked_identity.flow,
                checked_identity.source,
                item.record.record_id,
                Decimal(item.record.revision),
                item.record.content,
                checked_identity.run_epoch,
            )
            for item in dataset.records
        ]
        if upsert_params:
            await conn.executemany(
                f"""
                INSERT INTO {self._t_mirror}
                    (flow, source, record_id, revision, content, applied_epoch)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (flow, source, record_id) DO UPDATE
                SET revision = EXCLUDED.revision,
                    content = EXCLUDED.content,
                    applied_epoch = EXCLUDED.applied_epoch,
                    updated_at = clock_timestamp()
                """,
                upsert_params,
            )

        deleted_ids: list[str] = []
        if candidates:
            deleted_rows = await conn.fetch(
                f"""
                DELETE FROM {self._t_mirror}
                WHERE flow = $1 AND source = $2 AND record_id = ANY($3::text[])
                RETURNING record_id
                """,
                checked_identity.flow,
                checked_identity.source,
                [candidate.record_id for candidate in candidates],
            )
            deleted_ids = sorted(
                _row_text(deleted["record_id"], name="deleted record_id")
                for deleted in deleted_rows
            )
            if deleted_ids != [candidate.record_id for candidate in candidates]:
                raise ImportConcurrentMutation(
                    "mirror rows changed between anti-join inspection and deletion"
                )

        history_params = [
            (
                checked_identity.flow,
                checked_identity.source,
                checked_identity.run_epoch,
                checked_barrier,
                "upsert",
                item.record.record_id,
                item.ordinal,
                Decimal(item.record.revision),
                item.record.content,
            )
            for item in dataset.records
        ]
        history_params.extend(
            (
                checked_identity.flow,
                checked_identity.source,
                checked_identity.run_epoch,
                checked_barrier,
                "delete",
                candidate.record_id,
                None,
                Decimal(candidate.revision),
                candidate.content,
            )
            for candidate in candidates
        )
        if history_params:
            await conn.executemany(
                f"""
                INSERT INTO {self._t_history}
                    (flow, source, run_epoch, barrier, kind, record_id,
                     ordinal, revision, content)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                """,
                history_params,
            )

        await self._advance_watermark(
            conn,
            checked_identity,
            applied_through_ordinal=dataset.applied_through_ordinal,
            applied_cursor=dataset.applied_cursor,
        )
        settlement_row = await conn.fetchrow(
            f"""
            INSERT INTO {self._t_settlement}
                (flow, source, run_epoch, barrier, page_count, record_count,
                 payload_bytes, content_digest, upserted_count, deleted_count,
                 history_count, applied_through_ordinal, applied_cursor)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
            RETURNING *
            """,
            checked_identity.flow,
            checked_identity.source,
            checked_identity.run_epoch,
            checked_barrier,
            run.staged_pages,
            run.staged_records,
            run.staged_bytes,
            dataset.content_digest,
            len(dataset.records),
            len(candidates),
            len(history_params),
            dataset.applied_through_ordinal,
            dataset.applied_cursor,
        )
        if settlement_row is None:
            raise ImportStoreCorrupt("settlement insert returned no row")
        settled = await conn.fetchval(
            f"""
            UPDATE {self._t_run}
            SET state = 'settled', settled_barrier = $4,
                settled_at = clock_timestamp(), updated_at = clock_timestamp()
            WHERE flow = $1 AND source = $2 AND run_epoch = $3 AND state = 'staging'
            RETURNING 1
            """,
            checked_identity.flow,
            checked_identity.source,
            checked_identity.run_epoch,
            checked_barrier,
        )
        if settled != 1:
            raise ImportStoreCorrupt("run settlement did not update exactly one row")
        return CommitResult(settlement=_settlement_from_row(settlement_row), replayed=False)

    async def _lock_current(self, conn: asyncpg.Connection, identity: ImportIdentity) -> int:
        current_epoch = await self._lock_current_any(conn, identity)
        if current_epoch != identity.run_epoch:
            raise ImportStaleEpoch(identity.run_epoch, current_epoch)
        return current_epoch

    async def _lock_current_any(self, conn: asyncpg.Connection, identity: ImportIdentity) -> int:
        row = await conn.fetchrow(
            f"SELECT current_epoch FROM {self._t_current} "
            "WHERE flow = $1 AND source = $2 FOR UPDATE",
            identity.flow,
            identity.source,
        )
        if row is None:
            raise ImportRunMissing(
                f"no current run exists for {identity.flow!r}/{identity.source!r}"
            )
        return _row_int(
            row["current_epoch"],
            name="current_epoch",
            minimum=1,
            maximum=MAX_EPOCH,
        )

    async def _lock_run_for_write(
        self, conn: asyncpg.Connection, identity: ImportIdentity
    ) -> _RunRow:
        await self._lock_current(conn, identity)
        row = await conn.fetchrow(
            f"SELECT * FROM {self._t_run} "
            "WHERE flow = $1 AND source = $2 AND run_epoch = $3 FOR UPDATE",
            identity.flow,
            identity.source,
            identity.run_epoch,
        )
        if row is None:
            raise ImportRunMissing(f"current epoch {identity.run_epoch} has no persisted run row")
        return _run_from_row(row)

    async def _validate_dataset(self, conn: asyncpg.Connection, run: _RunRow) -> _ValidatedDataset:
        if not run.staged_complete or run.staged_through_cursor is not None:
            raise ImportSourceIncomplete("source staging is not terminally sealed")
        if run.staged_pages < 1 or run.staged_pages > self._max_pages:
            raise ImportBoundExceeded("pages", self._max_pages, run.staged_pages)
        if run.staged_records > self._max_records:
            raise ImportBoundExceeded("records", self._max_records, run.staged_records)
        if run.staged_bytes > self._max_total_bytes:
            raise ImportBoundExceeded("total_bytes", self._max_total_bytes, run.staged_bytes)
        page_rows = await conn.fetch(
            f"SELECT * FROM {self._t_page} "
            "WHERE flow = $1 AND source = $2 AND run_epoch = $3 ORDER BY page_no",
            run.identity.flow,
            run.identity.source,
            run.identity.run_epoch,
        )
        pages = tuple(_page_from_row(page) for page in page_rows)
        if len(pages) != run.staged_pages:
            raise ImportCorruptStaging(
                f"staged_pages is {run.staged_pages}, page rows are {len(pages)}"
            )
        if not pages:
            raise ImportCorruptStaging("a complete run has no stage page rows")
        record_rows = await conn.fetch(
            f"SELECT * FROM {self._t_record} "
            "WHERE flow = $1 AND source = $2 AND run_epoch = $3 ORDER BY ordinal",
            run.identity.flow,
            run.identity.source,
            run.identity.run_epoch,
        )
        records = tuple(_record_from_row(record) for record in record_rows)
        if len(records) != run.staged_records:
            raise ImportCorruptStaging(
                f"staged_records is {run.staged_records}, record rows are {len(records)}"
            )

        expected_page_no = 1
        expected_ordinal = 0
        record_index = 0
        previous_to_cursor: str | None = None
        previous_digest = _EMPTY_RUN_DIGEST
        total_bytes = 0
        for page in pages:
            if page.page_no != expected_page_no:
                raise ImportCorruptStaging("stage page numbers are not contiguous")
            if page.ordinal_start != expected_ordinal:
                raise ImportCorruptStaging("stage page ordinal ranges are not contiguous")
            if page.page_no == 1:
                if page.from_cursor is not None:
                    raise ImportCorruptStaging("first page must start from no cursor")
            elif page.from_cursor != previous_to_cursor:
                raise ImportCorruptStaging("stage page cursor chain is broken")
            if page.page_no < len(pages):
                if page.to_cursor is None:
                    raise ImportCorruptStaging("a nonterminal page has no next cursor")
                if page.record_count == 0:
                    raise ImportCorruptStaging("a nonterminal page is empty")
            else:
                if page.to_cursor is not None:
                    raise ImportCorruptStaging("terminal page carries a next cursor")
            page_records: list[ProviderRecord] = []
            for _ in range(page.record_count):
                if record_index >= len(records):
                    raise ImportCorruptStaging("page expects records that are absent")
                item = records[record_index]
                if item.ordinal != expected_ordinal or item.page_no != page.page_no:
                    raise ImportCorruptStaging("record ordinal/page is out of sequence")
                page_records.append(item.record)
                record_index += 1
                expected_ordinal += 1
            page_bytes = sum(len(_record_canonical_bytes(record)) for record in page_records)
            if page_bytes != page.payload_bytes:
                raise ImportCorruptStaging("page payload_bytes mismatch")
            if _page_digest(page.ordinal_start, page_records) != page.page_digest:
                raise ImportCorruptStaging("page digest mismatch")
            total_bytes += page_bytes
            previous_digest = _chain_digest(previous_digest, page.page_digest)
            previous_to_cursor = page.to_cursor
            expected_page_no += 1
        if record_index != len(records):
            raise ImportCorruptStaging("staged records extend past the final page")
        if expected_ordinal != run.staged_records:
            raise ImportCorruptStaging("staged record ordinals are not contiguous")
        if total_bytes != run.staged_bytes:
            raise ImportCorruptStaging("run staged_bytes mismatch")
        if previous_digest != run.staged_digest:
            raise ImportCorruptStaging("run aggregate content digest mismatch")
        return _ValidatedDataset(
            pages=pages,
            records=records,
            content_digest=previous_digest,
            applied_through_ordinal=run.staged_records - 1,
            applied_cursor=pages[-1].from_cursor,
        )

    async def _deletion_candidates(
        self, conn: asyncpg.Connection, identity: ImportIdentity
    ) -> tuple[_MirrorRow, ...]:
        rows = await conn.fetch(
            f"""
            SELECT m.record_id, m.revision, m.content
            FROM {self._t_mirror} AS m
            WHERE m.flow = $1 AND m.source = $2
              AND NOT EXISTS (
                  SELECT 1 FROM {self._t_record} AS s
                  WHERE s.flow = m.flow AND s.source = m.source
                    AND s.run_epoch = $3 AND s.record_id = m.record_id
              )
            ORDER BY m.record_id
            FOR UPDATE OF m
            """,
            identity.flow,
            identity.source,
            identity.run_epoch,
        )
        candidates: list[_MirrorRow] = []
        for row in rows:
            record_id = _row_text(row["record_id"], name="mirror record_id")
            if _RECORD_ID_PATTERN.fullmatch(record_id) is None:
                raise ImportStoreCorrupt("mirror record_id is malformed")
            content = _row_text(row["content"], name="mirror content")
            candidates.append(
                _MirrorRow(
                    record_id=record_id,
                    revision=_row_revision(row["revision"], name="mirror revision"),
                    content=content,
                )
            )
        return tuple(candidates)

    async def _advance_watermark(
        self,
        conn: asyncpg.Connection,
        identity: ImportIdentity,
        *,
        applied_through_ordinal: int,
        applied_cursor: str | None,
    ) -> None:
        row = await conn.fetchrow(
            f"SELECT * FROM {self._t_watermark} WHERE flow = $1 AND source = $2 FOR UPDATE",
            identity.flow,
            identity.source,
        )
        if row is None:
            await conn.execute(
                f"""
                INSERT INTO {self._t_watermark}
                    (flow, source, applied_epoch, applied_through_ordinal, applied_cursor)
                VALUES ($1, $2, $3, $4, $5)
                """,
                identity.flow,
                identity.source,
                identity.run_epoch,
                applied_through_ordinal,
                applied_cursor,
            )
            return
        watermark_epoch = _row_int(
            row["applied_epoch"], name="applied_epoch", minimum=1, maximum=MAX_EPOCH
        )
        watermark_ordinal = _row_int(
            row["applied_through_ordinal"],
            name="applied_through_ordinal",
            minimum=-1,
        )
        if watermark_epoch >= identity.run_epoch:
            raise ImportWatermarkRegression(
                f"watermark epoch {watermark_epoch} already covers requested epoch "
                f"{identity.run_epoch} without a matching settlement marker"
            )
        updated = await conn.fetchval(
            f"""
            UPDATE {self._t_watermark}
            SET applied_epoch = $3, applied_through_ordinal = $4,
                applied_cursor = $5, updated_at = clock_timestamp()
            WHERE flow = $1 AND source = $2 AND applied_epoch = $6
            RETURNING 1
            """,
            identity.flow,
            identity.source,
            identity.run_epoch,
            applied_through_ordinal,
            applied_cursor,
            watermark_epoch,
        )
        if updated != 1:
            raise ImportWatermarkRegression(
                f"watermark advanced concurrently from epoch {watermark_epoch} "
                f"(ordinal {watermark_ordinal})"
            )

    async def read_run(
        self, conn: asyncpg.Connection, identity: ImportIdentity
    ) -> ImportRunState | None:
        """Read one run state; no transaction is opened or retained."""
        checked = _validated_identity(identity)
        row = await conn.fetchrow(
            f"SELECT * FROM {self._t_run} WHERE flow = $1 AND source = $2 AND run_epoch = $3",
            checked.flow,
            checked.source,
            checked.run_epoch,
        )
        return None if row is None else _run_state(_run_from_row(row))

    async def read_current(self, conn: asyncpg.Connection, *, flow: str, source: str) -> int | None:
        """Read the committed current epoch pointer for one flow/source."""
        checked_flow, checked_source = _validated_source_key(flow, source)
        value = await conn.fetchval(
            f"SELECT current_epoch FROM {self._t_current} WHERE flow = $1 AND source = $2",
            checked_flow,
            checked_source,
        )
        if value is None:
            return None
        return _row_int(value, name="current_epoch", minimum=1, maximum=MAX_EPOCH)

    async def read_watermark(
        self, conn: asyncpg.Connection, *, flow: str, source: str
    ) -> ImportWatermark | None:
        """Read the committed applied watermark; absent until first settlement."""
        checked_flow, checked_source = _validated_source_key(flow, source)
        row = await conn.fetchrow(
            f"SELECT * FROM {self._t_watermark} WHERE flow = $1 AND source = $2",
            checked_flow,
            checked_source,
        )
        if row is None:
            return None
        return ImportWatermark(
            flow=_row_text(row["flow"], name="flow"),
            source=_row_text(row["source"], name="source"),
            applied_epoch=_row_int(
                row["applied_epoch"], name="applied_epoch", minimum=1, maximum=MAX_EPOCH
            ),
            applied_through_ordinal=_row_int(
                row["applied_through_ordinal"],
                name="applied_through_ordinal",
                minimum=-1,
            ),
            applied_cursor=_row_optional_text(row["applied_cursor"], name="applied_cursor"),
            updated_at=_row_time(row["updated_at"], name="updated_at"),
        )

    async def read_settlement(
        self, conn: asyncpg.Connection, identity: ImportIdentity, *, barrier: str
    ) -> ImportSettlement | None:
        """Read one settlement marker by stable identity, if committed."""
        checked = _validated_identity(identity)
        checked_barrier = _validated_barrier(barrier)
        row = await conn.fetchrow(
            f"SELECT * FROM {self._t_settlement} "
            "WHERE flow = $1 AND source = $2 AND run_epoch = $3 AND barrier = $4",
            checked.flow,
            checked.source,
            checked.run_epoch,
            checked_barrier,
        )
        return None if row is None else _settlement_from_row(row)


__all__ = [
    "DEFAULT_MAX_PAGES",
    "DEFAULT_MAX_PAGE_BYTES",
    "DEFAULT_MAX_PAGE_RECORDS",
    "DEFAULT_MAX_RECORDS",
    "DEFAULT_MAX_TOTAL_BYTES",
    "HARD_MAX_DELETIONS",
    "HARD_MAX_PAGES",
    "HARD_MAX_PAGE_BYTES",
    "HARD_MAX_PAGE_RECORDS",
    "HARD_MAX_RECORDS",
    "HARD_MAX_TOTAL_BYTES",
    "CommitResult",
    "ImportAlreadySettled",
    "ImportBoundExceeded",
    "ImportConcurrentMutation",
    "ImportContractError",
    "ImportCorruptStaging",
    "ImportCursorMismatch",
    "ImportEpochCASFailed",
    "ImportIdentity",
    "ImportMassDeleteRefused",
    "ImportOrdinalGap",
    "ImportPage",
    "ImportRunActive",
    "ImportRunMissing",
    "ImportRunNotOpen",
    "ImportRunState",
    "ImportSchemaError",
    "ImportSettlement",
    "ImportSourceIncomplete",
    "ImportStagingDiverged",
    "ImportStaleEpoch",
    "ImportStore",
    "ImportStoreCorrupt",
    "ImportStoreError",
    "ImportTransactionRequired",
    "ImportWatermark",
    "ImportWatermarkRegression",
    "QuarantineReason",
    "RunState",
    "StageProgress",
]

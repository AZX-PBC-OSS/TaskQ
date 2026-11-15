"""Generic local paged-provider fixture for the future sample application.

This module is **fixture support**, not an application. It defines the
provider protocol and a hermetic in-memory implementation so a future
standalone ``examples/`` paged-sync sample can exercise deterministic
pages, opaque cursors, bounded records/caches, and a fault script without
a network or database. It deliberately contains no workflow wiring, no
``map_index``, no idempotency keys, no hashing/modulo identity, and no
TaskQ engine imports.

Snapshot and cursor policy (explicit)
-------------------------------------
A scan starts when ``fetch(None, page_limit)`` is called. The fixture
takes an atomic snapshot of the records that exist at that moment and
sorts them by ``(record_id, revision)``. Every later page of that scan,
and every replay of one of its cursors, reads the same immutable
snapshot. Inserts or content revisions made after the scan started -
whether their key is before or after the current cursor - do not enter
that scan. A fresh scan sees the new state in full. This is a snapshot
cursor, not a live keyset: the scan boundary is explicit, so a mutation
behind the cursor cannot silently create a hole in one scan.

The cursor itself is opaque provider-issued data: ``pc1_<nonce>_<serial>``.
The nonce is instance-scoped (random by default). It never contains a
record id, revision, content, or ``map_index``, and it is never a
substitute for record identity. The server-side cursor state is a keyset
position ``(record_id, revision)`` (hidden from the caller).
Unknown, malformed, forged, foreign-provider, expired, and evicted
cursors fail closed with typed contract errors; they are never treated as
an ordinary empty/short page. Replaying a live cursor is safe and returns
the same page.

Bounds (no implicit unlimited backlog)
--------------------------------------
``max_records`` bounds the provider's mutable source (default 10,000,
hard maximum 200,000). ``max_page_size`` bounds every request (default
100, hard maximum 10,000), so page sizes are always 1..max_page_size.
``max_snapshots`` bounds retained scans and ``max_cursors`` bounds the
cursor registry; eviction is fail-closed and observable as a typed
``ProviderCursorExpired``/``ProviderUnknownCursor``. A fault script is
bounded to ``MAX_FAULT_RULES`` deterministic rules. The fixture opens no
socket, database, file, task, or other external resource; its only
synchronisation primitive is a plain ``threading.RLock``. It has no
``close()`` contract - dependency/resource lifecycle is a separate
engine concern and is not implemented or claimed here.

Faults and metrics (fixture-local only)
---------------------------------------
The fault script is keyed by the 1-based ``fetch`` call ordinal; it is
deterministic, never random. Every fetch invocation consumes an ordinal,
including rejected input and faults. Page-limit/cursor validation happens
before a scripted fault is applied, so malformed input can never be
masked as a scripted failure. ``before_page`` faults raise before a page
is handed back. ``after_page`` faults build a candidate page and then
raise, proving the caller cannot silently receive it; the candidate is
not counted as returned and its cursor is not registered. Fault
classification here (retryable unavailable/rate-limited, deterministic
poison) is fixture metadata only: it does not implement or prove engine
retry, backoff, ledger attempts, binding, parking, or release.

Exact transfer counters are exposed through ``ProviderStats``:
``fetch_calls`` counts every fetch invocation (including rejected input
and faults), ``fault_gates_tripped`` counts applied script rules,
``pages_returned``/``records_returned``/``payload_bytes`` count only
successfully returned pages (replays count again), and
``withheld_pages``/``withheld_payload_bytes`` describe after-page faults.
``payload_bytes`` for one record is the UTF-8 length of ``record_id`` and
``content`` plus 8 bytes for the exact revision integer.
"""

from __future__ import annotations

import math
import re
import secrets
import threading
from bisect import bisect_right
from collections import OrderedDict
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Annotated, ClassVar, Final, Literal, Protocol, Self, runtime_checkable

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    RootModel,
    ValidationError,
    field_validator,
    model_validator,
)

MAX_RECORD_ID_LENGTH: Final[int] = 128
MAX_RECORD_REVISION: Final[int] = 0xFFFFFFFFFFFFFFFF
MAX_RECORD_CONTENT_BYTES: Final[int] = 64 * 1024
MAX_CURSOR_LENGTH: Final[int] = 64
DEFAULT_MAX_RECORDS: Final[int] = 10_000
HARD_MAX_RECORDS: Final[int] = 200_000
DEFAULT_MAX_PAGE_SIZE: Final[int] = 100
HARD_MAX_PAGE_SIZE: Final[int] = 10_000
DEFAULT_MAX_SNAPSHOTS: Final[int] = 4
HARD_MAX_SNAPSHOTS: Final[int] = 1_024
DEFAULT_MAX_CURSORS: Final[int] = 256
HARD_MAX_CURSORS: Final[int] = 100_000
MAX_FAULT_RULES: Final[int] = 64
MAX_FAULT_ORDINAL: Final[int] = 1_000_000
RECORD_REVISION_BYTES: Final[int] = 8

_RECORD_ID_PATTERN: Final[str] = r"^[A-Za-z0-9._:-]+$"
_CURSOR_NONCE_SHAPE: Final[str] = r"^[0-9a-f]{16}$"
_CURSOR_SHAPE: Final[str] = r"^pc1_[0-9a-f]{16}_[0-9a-f]{16}$"
_CURSOR_SHAPE_RE: Final[re.Pattern[str]] = re.compile(_CURSOR_SHAPE)
_CURSOR_NONCE_SHAPE_RE: Final[re.Pattern[str]] = re.compile(_CURSOR_NONCE_SHAPE)


def _runtime_value(value: object) -> object:
    """Erase static narrowing for deliberate fail-closed runtime validation."""
    return value


def _validated_record(record: object) -> ProviderRecord:
    """Re-validate even a model instance so model_construct cannot smuggle bad data."""
    if not isinstance(record, ProviderRecord):
        raise ProviderMalformedRecord("records must be ProviderRecord instances")
    try:
        return ProviderRecord.model_validate(record.model_dump())
    except ValidationError as exc:
        raise ProviderMalformedRecord(f"invalid provider record: {exc}") from exc


class ProviderError(Exception):
    """Base class for fixture-local provider failures.

    ``retryable`` is fixture metadata, not an engine retry decision. No
    engine retry/backoff/ledger/parking behavior is implemented here.
    """

    retryable: ClassVar[bool] = False


class ProviderUnavailable(ProviderError):  # noqa: N818  # Why: required public fixture error vocabulary.
    """Ordinary transient provider failure (fixture classifies retryable)."""

    retryable: ClassVar[bool] = True

    def __init__(self, message: str = "provider unavailable") -> None:
        super().__init__(message)


class ProviderRateLimited(ProviderError):  # noqa: N818  # Why: required public fixture error vocabulary.
    """Advertised retry delay, carried as a finite nonnegative number."""

    retryable: ClassVar[bool] = True

    def __init__(self, delay_seconds: float, message: str | None = None) -> None:
        raw_delay = _runtime_value(delay_seconds)
        if (
            isinstance(raw_delay, bool)
            or not isinstance(raw_delay, (int, float))
            or not math.isfinite(float(raw_delay))
            or float(raw_delay) < 0.0
        ):
            raise ValueError("provider rate-limit delay must be finite and nonnegative")
        self.delay_seconds: float = float(raw_delay)
        super().__init__(
            message if message is not None else f"provider rate limited for {self.delay_seconds:g}s"
        )

    @property
    def retry_after_seconds(self) -> float:
        """Alias for the advertised delay; the fixture never sleeps on it."""
        return self.delay_seconds


class ProviderPoison(ProviderError):  # noqa: N818  # Why: required public fixture error vocabulary.
    """Deterministic, nonretryable provider/record poison."""

    retryable: ClassVar[bool] = False

    def __init__(self, message: str = "provider poison") -> None:
        super().__init__(message)


class ProviderContractError(ProviderError):
    """Fail-closed provider contract/cursor/record error (nonretryable)."""

    retryable: ClassVar[bool] = False


class ProviderUnknownCursor(ProviderContractError):  # noqa: N818  # Why: required public fixture error vocabulary.
    """Malformed, unissued, forged, foreign, or evicted cursor."""


class ProviderCursorExpired(ProviderUnknownCursor):
    """A valid issued cursor whose bounded snapshot has been released."""


class ProviderPageLimitError(ProviderContractError):
    """Requested page size is outside the configured 1..max bound."""


class ProviderRecordLimitExceeded(ProviderContractError):  # noqa: N818  # Why: required public fixture error vocabulary.
    """Mutable source would exceed the declared record bound."""


class ProviderMalformedRecord(ProviderContractError):  # noqa: N818  # Why: required public fixture error vocabulary.
    """Record input failed validation or violated stable-id/revision rules."""


class ProviderRecord(BaseModel):
    """One mutable provider record with a stable logical id and revision."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    record_id: str = Field(
        min_length=1,
        max_length=MAX_RECORD_ID_LENGTH,
        pattern=_RECORD_ID_PATTERN,
        strict=True,
    )
    revision: int = Field(ge=0, le=MAX_RECORD_REVISION, strict=True)
    content: str = Field(max_length=MAX_RECORD_CONTENT_BYTES, strict=True)

    @field_validator("content")
    @classmethod
    def _content_fits_byte_bound(cls, value: str) -> str:
        if len(value.encode("utf-8")) > MAX_RECORD_CONTENT_BYTES:
            raise ValueError("content exceeds MAX_RECORD_CONTENT_BYTES")
        return value

    @property
    def key(self) -> tuple[str, int]:
        """Deterministic keyset key: stable record id, then exact revision."""
        return (self.record_id, self.revision)

    @property
    def payload_bytes(self) -> int:
        """Canonical transfer size: id bytes + content bytes + 8-byte revision."""
        return (
            len(self.record_id.encode("utf-8"))
            + len(self.content.encode("utf-8"))
            + RECORD_REVISION_BYTES
        )


class ProviderCursor(RootModel[str]):
    """Opaque provider-issued scan cursor.

    Shape validation is necessary but not sufficient: only the issuing
    provider's live cursor registry can accept a token. The token exposes
    no record id, revision, content, or consumer identity.
    """

    model_config = ConfigDict(frozen=True)

    root: str = Field(
        min_length=len("pc1_") + 16 + 1 + 16,
        max_length=MAX_CURSOR_LENGTH,
        pattern=_CURSOR_SHAPE,
        strict=True,
    )

    @property
    def token(self) -> str:
        return self.root

    def __str__(self) -> str:
        return self.root


class ProviderUnavailableFault(BaseModel):
    """Deterministic before/after-page transient fault specification."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    kind: Literal["unavailable"] = "unavailable"
    message: str = Field(default="provider unavailable", min_length=1, max_length=256)


class ProviderRateLimitedFault(BaseModel):
    """Deterministic advertised-delay fault specification."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    kind: Literal["rate_limited"] = "rate_limited"
    delay_seconds: float = Field(ge=0.0)
    message: str = Field(default="provider rate limited", min_length=1, max_length=256)

    @field_validator("delay_seconds", mode="before")
    @classmethod
    def _delay_is_not_bool(cls, value: object) -> object:
        if isinstance(value, bool):
            raise ValueError("rate-limit delay must be a number, not bool")
        return value

    @field_validator("delay_seconds")
    @classmethod
    def _delay_is_finite(cls, value: float) -> float:
        if not math.isfinite(value):
            raise ValueError("rate-limit delay must be finite")
        return value


class ProviderPoisonFault(BaseModel):
    """Deterministic nonretryable poison fault specification."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    kind: Literal["poison"] = "poison"
    message: str = Field(default="provider poison", min_length=1, max_length=256)


type ProviderFault = Annotated[
    ProviderUnavailableFault | ProviderRateLimitedFault | ProviderPoisonFault,
    Field(discriminator="kind"),
]
type ProviderFaultPhase = Literal["before_page", "after_page"]


class ProviderFaultRule(BaseModel):
    """One deterministic rule keyed by the 1-based fetch operation ordinal."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    operation: Literal["fetch"] = "fetch"
    ordinal: int = Field(ge=1, le=MAX_FAULT_ORDINAL, strict=True)
    phase: ProviderFaultPhase = "before_page"
    fault: ProviderFault


def _exception_for_fault(fault: ProviderFault) -> ProviderError:
    raw_fault = _runtime_value(fault)
    if isinstance(raw_fault, ProviderUnavailableFault):
        return ProviderUnavailable(raw_fault.message)
    if isinstance(raw_fault, ProviderRateLimitedFault):
        return ProviderRateLimited(raw_fault.delay_seconds, raw_fault.message)
    if isinstance(raw_fault, ProviderPoisonFault):
        return ProviderPoison(raw_fault.message)
    raise ProviderContractError(f"unknown provider fault kind: {type(raw_fault).__name__}")


class ProviderFaultScript:
    """Immutable, bounded, ordinal-keyed fault plan; unknown input fails closed."""

    __slots__ = ("_by_ordinal", "_rules")

    def __init__(self, rules: Iterable[ProviderFaultRule] = ()) -> None:
        seen: set[int] = set()
        valid: list[ProviderFaultRule] = []
        for supplied, rule in enumerate(rules, start=1):
            if supplied > MAX_FAULT_RULES:
                raise ProviderContractError(f"fault script exceeds {MAX_FAULT_RULES} rules")
            raw_rule = _runtime_value(rule)
            if not isinstance(raw_rule, ProviderFaultRule):
                raise ProviderContractError("fault rules must be ProviderFaultRule instances")
            if rule.operation != "fetch":
                raise ProviderContractError(f"unknown provider operation: {rule.operation!r}")
            if rule.phase not in ("before_page", "after_page"):
                raise ProviderContractError(f"unknown fault phase: {rule.phase!r}")
            if type(rule.ordinal) is not int or not 1 <= rule.ordinal <= MAX_FAULT_ORDINAL:
                raise ProviderContractError("fault ordinal is outside the declared bound")
            if rule.ordinal in seen:
                raise ProviderContractError(f"duplicate fault ordinal: {rule.ordinal}")
            seen.add(rule.ordinal)
            try:
                _exception_for_fault(rule.fault)
            except (ProviderContractError, ValueError) as exc:
                raise ProviderContractError(f"invalid fault at ordinal {rule.ordinal}") from exc
            valid.append(rule)
        valid.sort(key=lambda rule: rule.ordinal)
        self._rules: tuple[ProviderFaultRule, ...] = tuple(valid)
        self._by_ordinal: dict[int, ProviderFaultRule] = {rule.ordinal: rule for rule in valid}

    @property
    def rules(self) -> tuple[ProviderFaultRule, ...]:
        return self._rules

    def rule_for(self, ordinal: int) -> ProviderFaultRule | None:
        if type(ordinal) is not int:
            return None
        return self._by_ordinal.get(ordinal)


class ProviderPage(BaseModel):
    """One deterministic page: immutable records, opaque next cursor, terminal flag."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    records: tuple[ProviderRecord, ...]
    next_cursor: ProviderCursor | None = None
    has_more: bool

    @model_validator(mode="after")
    def _page_is_coherent(self) -> Self:
        keys = [record.key for record in self.records]
        if keys != sorted(keys):
            raise ValueError("page records must be in deterministic keyset order")
        ids = [record.record_id for record in self.records]
        if len(ids) != len(set(ids)):
            raise ValueError("page contains a duplicate record_id")
        if self.has_more and not self.records:
            raise ValueError("a page with has_more=True cannot be empty")
        if self.has_more != (self.next_cursor is not None):
            raise ValueError("has_more and next_cursor must agree")
        return self

    @property
    def payload_bytes(self) -> int:
        return sum(record.payload_bytes for record in self.records)


class ProviderStats(BaseModel):
    """Immutable exact transfer/fault counters for one provider instance."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    fetch_calls: int = Field(default=0, ge=0)
    fault_gates_tripped: int = Field(default=0, ge=0)
    pages_returned: int = Field(default=0, ge=0)
    records_returned: int = Field(default=0, ge=0)
    payload_bytes: int = Field(default=0, ge=0)
    withheld_pages: int = Field(default=0, ge=0)
    withheld_payload_bytes: int = Field(default=0, ge=0)
    cursors_issued: int = Field(default=0, ge=0)
    snapshots_created: int = Field(default=0, ge=0)

    @property
    def faults_raised(self) -> int:
        """Alias: every tripped gate raises exactly one fixture fault."""
        return self.fault_gates_tripped


@runtime_checkable
class PagedProvider(Protocol):
    """Async paged-provider protocol used by the future sample application."""

    async def fetch(self, cursor: ProviderCursor | None, page_limit: int) -> ProviderPage:
        """Return the next page; any failure is an explicit typed exception."""
        ...


@dataclass(frozen=True, slots=True)
class _Snapshot:
    snapshot_id: int
    records: tuple[ProviderRecord, ...]
    keys: tuple[tuple[str, int], ...]


@dataclass(frozen=True, slots=True)
class _CursorState:
    snapshot_id: int
    last_key: tuple[str, int]


class LocalPagedProvider:
    """Hermetic, bounded, deterministic implementation of ``PagedProvider``.

    Control-plane methods (``replace_records``/``upsert``) are synchronous
    local fixture mutations; ``fetch`` is async but performs no I/O and
    never awaits. All shared state is guarded by one plain ``threading``
    lock, so concurrent tasks, threads, and separate ``asyncio.run`` loops
    can share the fixture without loop-bound primitives.
    """

    def __init__(
        self,
        records: Iterable[ProviderRecord] = (),
        *,
        max_records: int = DEFAULT_MAX_RECORDS,
        max_page_size: int = DEFAULT_MAX_PAGE_SIZE,
        max_snapshots: int = DEFAULT_MAX_SNAPSHOTS,
        max_cursors: int = DEFAULT_MAX_CURSORS,
        cursor_nonce: str | None = None,
        fault_script: ProviderFaultScript | None = None,
    ) -> None:
        self._max_records = self._bounded_int(
            max_records, name="max_records", minimum=1, maximum=HARD_MAX_RECORDS
        )
        self._max_page_size = self._bounded_int(
            max_page_size, name="max_page_size", minimum=1, maximum=HARD_MAX_PAGE_SIZE
        )
        self._max_snapshots = self._bounded_int(
            max_snapshots, name="max_snapshots", minimum=1, maximum=HARD_MAX_SNAPSHOTS
        )
        self._max_cursors = self._bounded_int(
            max_cursors, name="max_cursors", minimum=1, maximum=HARD_MAX_CURSORS
        )
        if cursor_nonce is None:
            self._cursor_nonce = secrets.token_hex(8)
        else:
            raw_nonce = _runtime_value(cursor_nonce)
            if (
                not isinstance(raw_nonce, str)
                or _CURSOR_NONCE_SHAPE_RE.fullmatch(raw_nonce) is None
            ):
                raise ProviderContractError("cursor_nonce must be 16 lowercase hex characters")
            self._cursor_nonce = raw_nonce
        raw_script = _runtime_value(fault_script)
        if raw_script is None:
            self._fault_script = ProviderFaultScript()
        elif isinstance(raw_script, ProviderFaultScript):
            self._fault_script = raw_script
        else:
            raise ProviderContractError("fault_script must be a ProviderFaultScript")

        self._lock = threading.RLock()
        self._records: dict[str, ProviderRecord] = {}
        self._snapshots: OrderedDict[int, _Snapshot] = OrderedDict()
        self._cursors: OrderedDict[str, _CursorState] = OrderedDict()
        self._cursor_tokens: dict[tuple[int, tuple[str, int]], str] = {}
        self._next_snapshot_id = 1
        self._next_cursor_serial = 1

        self._fetch_calls = 0
        self._fault_gates_tripped = 0
        self._pages_returned = 0
        self._records_returned = 0
        self._payload_bytes = 0
        self._withheld_pages = 0
        self._withheld_payload_bytes = 0
        self._cursors_issued = 0
        self._snapshots_created = 0

        self.replace_records(records)

    @staticmethod
    def _bounded_int(value: int, *, name: str, minimum: int, maximum: int) -> int:
        if type(value) is not int or not minimum <= value <= maximum:
            raise ProviderContractError(f"{name} must be an exact int in [{minimum}, {maximum}]")
        return value

    @property
    def record_count(self) -> int:
        with self._lock:
            return len(self._records)

    @property
    def stats(self) -> ProviderStats:
        with self._lock:
            return ProviderStats(
                fetch_calls=self._fetch_calls,
                fault_gates_tripped=self._fault_gates_tripped,
                pages_returned=self._pages_returned,
                records_returned=self._records_returned,
                payload_bytes=self._payload_bytes,
                withheld_pages=self._withheld_pages,
                withheld_payload_bytes=self._withheld_payload_bytes,
                cursors_issued=self._cursors_issued,
                snapshots_created=self._snapshots_created,
            )

    @property
    def fault_script(self) -> ProviderFaultScript:
        return self._fault_script

    def current_records(self) -> tuple[ProviderRecord, ...]:
        """Current source state (control-plane view, not a provider page)."""
        with self._lock:
            return self._sorted_records_locked()

    def replace_records(self, records: Iterable[ProviderRecord]) -> None:
        """Atomically replace the bounded source and invalidate old cursors."""
        with self._lock:
            materialized: list[ProviderRecord] = []
            seen: set[str] = set()
            for record in records:
                if len(materialized) >= self._max_records:
                    raise ProviderRecordLimitExceeded(
                        f"source would exceed the bound of {self._max_records} records"
                    )
                candidate = _validated_record(record)
                if candidate.record_id in seen:
                    raise ProviderMalformedRecord(
                        f"duplicate record_id in source: {candidate.record_id!r}"
                    )
                seen.add(candidate.record_id)
                materialized.append(candidate)
            replacement = {record.record_id: record for record in materialized}
            self._records = replacement
            self._snapshots.clear()
            self._cursors.clear()
            self._cursor_tokens.clear()

    def upsert(
        self,
        record_id: str,
        content: str,
        *,
        revision: int | None = None,
    ) -> ProviderRecord:
        """Insert or revise one record; revisions are monotonic and fail closed."""
        with self._lock:
            if revision is not None and type(revision) is not int:
                raise ProviderMalformedRecord("revision must be an exact nonnegative int")
            existing = self._records.get(record_id)
            if existing is None:
                if len(self._records) >= self._max_records:
                    raise ProviderRecordLimitExceeded(
                        f"source bound of {self._max_records} records would be exceeded"
                    )
                next_revision = 0 if revision is None else revision
                record = self._build_record(record_id, next_revision, content)
                self._records[record_id] = record
                return record

            next_revision = existing.revision + 1 if revision is None else revision
            if next_revision < existing.revision:
                raise ProviderMalformedRecord(
                    f"revision {next_revision} is stale for {record_id!r} "
                    f"(current {existing.revision})"
                )
            if next_revision == existing.revision:
                if content != existing.content:
                    raise ProviderMalformedRecord(
                        f"revision {next_revision} already exists with different content"
                    )
                return existing
            record = self._build_record(record_id, next_revision, content)
            self._records[record_id] = record
            return record

    async def fetch(self, cursor: ProviderCursor | None, page_limit: int) -> ProviderPage:
        with self._lock:
            self._fetch_calls += 1
            ordinal = self._fetch_calls
            self._validate_page_limit_locked(page_limit)

            raw_cursor = _runtime_value(cursor)
            if raw_cursor is None:
                records = self._sorted_records_locked()
                keys = tuple(record.key for record in records)
                snapshot_id: int | None = None
                start = 0
            else:
                if not isinstance(raw_cursor, ProviderCursor):
                    raise ProviderUnknownCursor("cursor must be a ProviderCursor")
                token = raw_cursor.root
                if _CURSOR_SHAPE_RE.fullmatch(token) is None:
                    raise ProviderUnknownCursor("cursor is malformed")
                state = self._cursors.get(token)
                if state is None:
                    raise ProviderUnknownCursor("cursor was not issued by this provider")
                snapshot = self._snapshots.get(state.snapshot_id)
                if snapshot is None:
                    raise ProviderCursorExpired("cursor's snapshot has been released")
                self._cursors.move_to_end(token)
                records = snapshot.records
                keys = snapshot.keys
                snapshot_id = snapshot.snapshot_id
                start = bisect_right(keys, state.last_key)

            end = min(start + page_limit, len(records))
            page_records = records[start:end]
            has_more = end < len(records)
            next_key = page_records[-1].key if has_more else None

            rule = self._fault_script.rule_for(ordinal)
            if rule is not None:
                self._fault_gates_tripped += 1
                exception = _exception_for_fault(rule.fault)
                if rule.phase == "before_page":
                    raise exception
                self._withheld_pages += 1
                self._withheld_payload_bytes += sum(record.payload_bytes for record in page_records)
                raise exception

            if snapshot_id is None and has_more:
                snapshot_id = self._store_snapshot_locked(records, keys)
            next_cursor: ProviderCursor | None = None
            if has_more:
                assert snapshot_id is not None
                assert next_key is not None
                next_cursor = self._issue_cursor_locked(snapshot_id, next_key)
            page = ProviderPage(
                records=page_records,
                next_cursor=next_cursor,
                has_more=has_more,
            )
            self._pages_returned += 1
            self._records_returned += len(page_records)
            self._payload_bytes += page.payload_bytes
            return page

    def _validate_page_limit_locked(self, page_limit: int) -> None:
        if type(page_limit) is not int or not 1 <= page_limit <= self._max_page_size:
            raise ProviderPageLimitError(
                f"page_limit must be an exact int in [1, {self._max_page_size}]"
            )

    def _sorted_records_locked(self) -> tuple[ProviderRecord, ...]:
        return tuple(sorted(self._records.values(), key=lambda record: record.key))

    def _build_record(self, record_id: str, revision: int, content: str) -> ProviderRecord:
        try:
            return ProviderRecord(record_id=record_id, revision=revision, content=content)
        except ValidationError as exc:
            raise ProviderMalformedRecord(f"invalid provider record: {exc}") from exc

    def _store_snapshot_locked(
        self,
        records: tuple[ProviderRecord, ...],
        keys: tuple[tuple[str, int], ...],
    ) -> int:
        snapshot_id = self._next_snapshot_id
        self._next_snapshot_id += 1
        snapshot = _Snapshot(snapshot_id=snapshot_id, records=records, keys=keys)
        while len(self._snapshots) >= self._max_snapshots:
            self._snapshots.popitem(last=False)
        self._snapshots[snapshot_id] = snapshot
        self._snapshots_created += 1
        return snapshot_id

    def _issue_cursor_locked(
        self,
        snapshot_id: int,
        last_key: tuple[str, int],
    ) -> ProviderCursor:
        state_key = (snapshot_id, last_key)
        existing_token = self._cursor_tokens.get(state_key)
        if existing_token is not None and existing_token in self._cursors:
            self._cursors.move_to_end(existing_token)
            return ProviderCursor(existing_token)

        serial = self._next_cursor_serial
        if serial > 0xFFFFFFFFFFFFFFFF:
            raise ProviderContractError("cursor serial exhausted")
        self._next_cursor_serial = serial + 1
        token = f"pc1_{self._cursor_nonce}_{serial:016x}"
        self._cursors[token] = _CursorState(snapshot_id=snapshot_id, last_key=last_key)
        self._cursor_tokens[state_key] = token
        self._cursors_issued += 1
        while len(self._cursors) > self._max_cursors:
            evicted_token, evicted_state = self._cursors.popitem(last=False)
            evicted_key = (evicted_state.snapshot_id, evicted_state.last_key)
            if self._cursor_tokens.get(evicted_key) == evicted_token:
                del self._cursor_tokens[evicted_key]
        return ProviderCursor(token)


__all__ = [
    "DEFAULT_MAX_CURSORS",
    "DEFAULT_MAX_PAGE_SIZE",
    "DEFAULT_MAX_RECORDS",
    "DEFAULT_MAX_SNAPSHOTS",
    "HARD_MAX_CURSORS",
    "HARD_MAX_PAGE_SIZE",
    "HARD_MAX_RECORDS",
    "HARD_MAX_SNAPSHOTS",
    "MAX_CURSOR_LENGTH",
    "MAX_FAULT_ORDINAL",
    "MAX_FAULT_RULES",
    "MAX_RECORD_CONTENT_BYTES",
    "MAX_RECORD_ID_LENGTH",
    "MAX_RECORD_REVISION",
    "RECORD_REVISION_BYTES",
    "LocalPagedProvider",
    "PagedProvider",
    "ProviderContractError",
    "ProviderCursor",
    "ProviderCursorExpired",
    "ProviderError",
    "ProviderFault",
    "ProviderFaultPhase",
    "ProviderFaultRule",
    "ProviderFaultScript",
    "ProviderMalformedRecord",
    "ProviderPage",
    "ProviderPageLimitError",
    "ProviderPoison",
    "ProviderPoisonFault",
    "ProviderRateLimited",
    "ProviderRateLimitedFault",
    "ProviderRecord",
    "ProviderRecordLimitExceeded",
    "ProviderStats",
    "ProviderUnavailable",
    "ProviderUnavailableFault",
    "ProviderUnknownCursor",
]

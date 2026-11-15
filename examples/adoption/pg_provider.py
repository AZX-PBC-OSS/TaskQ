# ruff: noqa: S608  # Why: every SQL table name is a module constant and the only interpolated identifier is the validated scratch schema/constant table name; all caller values are $N-bound.
"""Generic real-PostgreSQL paged-provider fixture for a future sample application.

This module is **fixture support**, not an application. It implements the
same ``PagedProvider`` protocol as ``examples.adoption.provider`` on a real
PostgreSQL server, using the accepted fixture's ``ProviderRecord``,
``ProviderPage``, ``ProviderCursor``, fault-script, stats, and typed error
classes. It contains no TaskQ workflow/engine imports, no ``map_index``, no
engine migration, and no public TaskQ API. It is deliberately not wired into
``deps_factory`` or any worker.

Why a PostgreSQL variant
------------------------
The accepted local fixture keeps its snapshot and cursor registries in one
process's memory. A real fleet chain source can park on one worker and resume
on another worker/process with a new provider instance and new connections.
A per-instance in-memory registry cannot accept that foreign cursor. This
fixture therefore persists the source records, immutable snapshot copies,
opaque cursors, positions, retention bounds, counters, and the fault script
in a caller-supplied **fixture-owned scratch schema**. Any provider instance
constructed for the same persisted scenario sees the same state.

Storage and transaction policy (explicit)
-----------------------------------------
The caller supplies a typed ``asyncpg.Pool`` and a validated lowercase
scratch schema identifier. ``initialize()`` creates only the fixture's own
tables in that schema with idempotent ``CREATE TABLE IF NOT EXISTS`` /
``CREATE INDEX IF NOT EXISTS`` statements; it never applies a TaskQ migration,
never touches another schema, and never alters an incompatible pre-existing
table. An incompatible pre-existing shape fails closed with
``ProviderContractError`` and a bounded diagnostic so a test can drop the
scratch schema/table and retry instead of being blocked forever. A
pre-existing scenario row's nonce/bounds/fault-script/count fields are
validated on initialization; record and snapshot rows are revalidated when
read, so tampered or invalid row documents fail closed with the existing
typed record/cursor error instead of being returned as a page.

Initialization separates short DDL from scenario DML. The schema-scoped
PostgreSQL advisory transaction lock serializes only the idempotent bootstrap
DDL (the hash is a lock domain, never an identity); that transaction commits
before any scenario row transaction starts. When the complete fixture shape
and indexes already exist, DDL is skipped. Concurrent initializers therefore
do not hold DDL ShareLocks while waiting for scenario DML locks, and an
in-flight fetch on another scenario cannot be a deadlock victim of a joining
instance. Fresh-schema bootstrap is serialized under the same lock. Any
unexpected server/client setup driver error is translated to
``ProviderContractError`` with the original exception preserved as
``__cause__``.

Every ``fetch`` acquires one pool connection and one explicit transaction,
finishes or rolls back that transaction, and only then returns/raises. No
ambient caller transaction is required or retained; no transaction is left
open on the connection. Successful pages commit their counters together with
snapshot/cursor writes. A scripted fault commits only its fetch/fault
counters, then raises the accepted typed fault outside the transaction.
Invalid input also commits its fetch ordinal before raising. An internal
contract error (for example a tampered source count) rolls the whole fetch
transaction back and therefore does not consume an ordinal. There is no
timer, TTL, background task, or 50ms polling loop: retention is
changed only by explicit bounded eviction during a later successful
snapshot/cursor allocation, and by explicit control-plane replacement or the
owner dropping the scratch schema after the run.

Snapshot and cursor policy
--------------------------
The first page of a scan pins the current bounded record set by copying it
into an immutable snapshot under the scenario lock. Later pages and replays
read only that snapshot, so inserts and content revisions made after the
scan started never enter it. A fresh scan sees the new source state.

Each record row has an explicitly allocated, scenario-unique ordinal. The
ordinal is allocated when the record id is first inserted, is preserved on
content revision, and is copied into snapshots. It is a provider record
identity candidate for a future sample; it is **not** a ``map_index``, a
cursor position, or an engine identity. Page order is deterministic
``(record_id, revision)`` exactly like the accepted local fixture.

The cursor token remains opaque provider data: ``pc1_<scenario-nonce>_<serial>``.
It contains no record id/content/ordinal and exposes no PostgreSQL position.
The server-side cursor row stores the snapshot id plus the last returned
``(record_id, revision)``. Cursor lookup is scoped by scenario id, and a
nonce is persisted uniquely across the fixture schema: two provider handles
may share one nonce only when they are genuinely the same persisted
scenario; a different scenario requesting an already-persisted nonce fails
closed with ``ProviderContractError``. This is a safer PostgreSQL fixture
rule than the accepted local fixture's documented same-explicit-nonce
caller foot-gun, and it means token text is never ambiguous across
scenarios. Replaying a cursor returns the same page and re-issues the same
next-cursor token while that cursor row is retained; eviction/expiry is a
typed error, never an ordinary empty page.

Bounds, faults, and metrics
---------------------------
The accepted local bounds are reused: ``DEFAULT_MAX_RECORDS`` (hard 200,000),
page size (hard 10,000), ``DEFAULT_MAX_SNAPSHOTS`` (hard 1,024),
``DEFAULT_MAX_CURSORS`` (hard 100,000), and ``MAX_FAULT_RULES``. Snapshot
record copies are bounded by retained snapshots x records; cursor rows are
bounded by ``max_cursors``; the source is bounded by ``max_records``. A long
replay source cannot grow an unrestricted table.

The fault script is persisted with the scenario so an independently
constructed provider instance/pool evaluates the same rules against the same
durable 1-based fetch ordinal. Fault classes are the accepted fixture's
``ProviderUnavailable`` (retryable), ``ProviderRateLimited`` (finite
nonnegative advertised delay, never slept on), and ``ProviderPoison``
(nonretryable); after-page faults withhold the candidate page and its cursor.
Counters are stored and incremented under the scenario row lock, so
concurrent fetches across instances/connections have no lost updates.
Script equality is checked through the typed ``ProviderFaultScript`` rules,
not raw JSON text, so jsonb's numeric rendering (for example ``1e16`` becoming
plain decimal) round-trips without a false mismatch. Persisted rule payloads
are type-checked before model coercion: non-finite, non-numeric, bool, or
string delays, unknown kinds, and wrong numeric rule fields fail closed.

No exact-once claim follows from these record-safe counters. External side
effects and engine attempts are outside this fixture. Boundary candidates,
P4/P5 policy, inline-node policy, terminal retention, live workers, and any
runnable sample are future work, not proven here.
"""

from __future__ import annotations

import json
import math
import re
import secrets
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import Final, cast

import asyncpg
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from examples.adoption.provider import (
    DEFAULT_MAX_CURSORS,
    DEFAULT_MAX_PAGE_SIZE,
    DEFAULT_MAX_RECORDS,
    DEFAULT_MAX_SNAPSHOTS,
    HARD_MAX_CURSORS,
    HARD_MAX_PAGE_SIZE,
    HARD_MAX_RECORDS,
    HARD_MAX_SNAPSHOTS,
    MAX_FAULT_RULES,
    MAX_RECORD_CONTENT_BYTES,
    MAX_RECORD_ID_LENGTH,
    MAX_RECORD_REVISION,
    ProviderContractError,
    ProviderCursor,
    ProviderCursorExpired,
    ProviderError,
    ProviderFault,
    ProviderFaultRule,
    ProviderFaultScript,
    ProviderMalformedRecord,
    ProviderPage,
    ProviderPageLimitError,
    ProviderPoison,
    ProviderPoisonFault,
    ProviderRateLimited,
    ProviderRateLimitedFault,
    ProviderRecord,
    ProviderRecordLimitExceeded,
    ProviderStats,
    ProviderUnavailable,
    ProviderUnavailableFault,
    ProviderUnknownCursor,
)

_SCHEMA_RE: Final[re.Pattern[str]] = re.compile(r"^[a-z][a-z0-9_]{0,62}$")
_SCENARIO_RE: Final[str] = r"^[A-Za-z0-9._:-]{1,128}$"
_RECORD_ID_PATTERN: Final[str] = r"^[A-Za-z0-9._:-]+$"

_T_SCENARIO: Final[str] = "adoption_provider_scenario"
_T_RECORD: Final[str] = "adoption_provider_record"
_T_SNAPSHOT: Final[str] = "adoption_provider_snapshot"
_T_SNAPSHOT_RECORD: Final[str] = "adoption_provider_snapshot_record"
_T_CURSOR: Final[str] = "adoption_provider_cursor"

_EXPECTED_COLUMNS: Final[dict[str, frozenset[str]]] = {
    _T_SCENARIO: frozenset(
        {
            "scenario_id",
            "cursor_nonce",
            "max_records",
            "max_page_size",
            "max_snapshots",
            "max_cursors",
            "next_ordinal",
            "next_snapshot_id",
            "next_cursor_serial",
            "next_use_serial",
            "record_count",
            "snapshot_count",
            "cursor_count",
            "fetch_calls",
            "fault_gates_tripped",
            "pages_returned",
            "records_returned",
            "payload_bytes",
            "withheld_pages",
            "withheld_payload_bytes",
            "cursors_issued",
            "snapshots_created",
            "fault_script",
        }
    ),
    _T_RECORD: frozenset({"scenario_id", "ordinal", "record_id", "revision", "content"}),
    _T_SNAPSHOT: frozenset({"scenario_id", "snapshot_id", "record_count"}),
    _T_SNAPSHOT_RECORD: frozenset(
        {"scenario_id", "snapshot_id", "ordinal", "record_id", "revision", "content"}
    ),
    _T_CURSOR: frozenset(
        {
            "scenario_id",
            "token",
            "snapshot_id",
            "last_record_id",
            "last_revision",
            "last_used_seq",
            "issued_seq",
        }
    ),
}

_EXPECTED_TYPES: Final[dict[str, dict[str, str]]] = {
    _T_SCENARIO: {
        "scenario_id": "text",
        "cursor_nonce": "text",
        "max_records": "integer",
        "max_page_size": "integer",
        "max_snapshots": "integer",
        "max_cursors": "integer",
        "next_ordinal": "bigint",
        "next_snapshot_id": "bigint",
        "next_cursor_serial": "bigint",
        "next_use_serial": "bigint",
        "record_count": "integer",
        "snapshot_count": "integer",
        "cursor_count": "integer",
        "fetch_calls": "bigint",
        "fault_gates_tripped": "bigint",
        "pages_returned": "bigint",
        "records_returned": "bigint",
        "payload_bytes": "bigint",
        "withheld_pages": "bigint",
        "withheld_payload_bytes": "bigint",
        "cursors_issued": "bigint",
        "snapshots_created": "bigint",
        "fault_script": "jsonb",
    },
    _T_RECORD: {
        "scenario_id": "text",
        "ordinal": "bigint",
        "record_id": "text",
        "revision": "numeric",
        "content": "text",
    },
    _T_SNAPSHOT: {
        "scenario_id": "text",
        "snapshot_id": "bigint",
        "record_count": "integer",
    },
    _T_SNAPSHOT_RECORD: {
        "scenario_id": "text",
        "snapshot_id": "bigint",
        "ordinal": "bigint",
        "record_id": "text",
        "revision": "numeric",
        "content": "text",
    },
    _T_CURSOR: {
        "scenario_id": "text",
        "token": "text",
        "snapshot_id": "bigint",
        "last_record_id": "text",
        "last_revision": "numeric",
        "last_used_seq": "bigint",
        "issued_seq": "bigint",
    },
}

_REQUIRED_INDEX_NAMES: Final[tuple[str, ...]] = (
    f"{_T_SCENARIO}_id_uidx",
    f"{_T_SCENARIO}_nonce_uidx",
    f"{_T_RECORD}_ordinal_uidx",
    f"{_T_RECORD}_id_uidx",
    f"{_T_SNAPSHOT}_id_uidx",
    f"{_T_SNAPSHOT_RECORD}_ordinal_uidx",
    f"{_T_SNAPSHOT_RECORD}_id_uidx",
    f"{_T_CURSOR}_token_uidx",
    f"{_T_CURSOR}_position_uidx",
    f"{_T_RECORD}_order_idx",
    f"{_T_SNAPSHOT_RECORD}_order_idx",
    f"{_T_CURSOR}_lru_idx",
    f"{_T_SNAPSHOT}_lru_idx",
)

_DDL_STATEMENTS: Final[tuple[str, ...]] = (
    f"""
    CREATE TABLE IF NOT EXISTS {_T_SCENARIO} (
        scenario_id text NOT NULL PRIMARY KEY,
        cursor_nonce text NOT NULL UNIQUE CHECK (cursor_nonce ~ '^[0-9a-f]{{16}}$'),
        max_records integer NOT NULL CHECK (max_records BETWEEN 1 AND {HARD_MAX_RECORDS}),
        max_page_size integer NOT NULL CHECK (max_page_size BETWEEN 1 AND {HARD_MAX_PAGE_SIZE}),
        max_snapshots integer NOT NULL CHECK (max_snapshots BETWEEN 1 AND {HARD_MAX_SNAPSHOTS}),
        max_cursors integer NOT NULL CHECK (max_cursors BETWEEN 1 AND {HARD_MAX_CURSORS}),
        next_ordinal bigint NOT NULL DEFAULT 1 CHECK (next_ordinal >= 1),
        next_snapshot_id bigint NOT NULL DEFAULT 1 CHECK (next_snapshot_id >= 1),
        next_cursor_serial bigint NOT NULL DEFAULT 1 CHECK (next_cursor_serial >= 1),
        next_use_serial bigint NOT NULL DEFAULT 1 CHECK (next_use_serial >= 1),
        record_count integer NOT NULL DEFAULT 0
            CHECK (record_count >= 0 AND record_count <= max_records),
        snapshot_count integer NOT NULL DEFAULT 0
            CHECK (snapshot_count >= 0 AND snapshot_count <= max_snapshots),
        cursor_count integer NOT NULL DEFAULT 0
            CHECK (cursor_count >= 0 AND cursor_count <= max_cursors),
        fetch_calls bigint NOT NULL DEFAULT 0 CHECK (fetch_calls >= 0),
        fault_gates_tripped bigint NOT NULL DEFAULT 0 CHECK (fault_gates_tripped >= 0),
        pages_returned bigint NOT NULL DEFAULT 0 CHECK (pages_returned >= 0),
        records_returned bigint NOT NULL DEFAULT 0 CHECK (records_returned >= 0),
        payload_bytes bigint NOT NULL DEFAULT 0 CHECK (payload_bytes >= 0),
        withheld_pages bigint NOT NULL DEFAULT 0 CHECK (withheld_pages >= 0),
        withheld_payload_bytes bigint NOT NULL DEFAULT 0 CHECK (withheld_payload_bytes >= 0),
        cursors_issued bigint NOT NULL DEFAULT 0 CHECK (cursors_issued >= 0),
        snapshots_created bigint NOT NULL DEFAULT 0 CHECK (snapshots_created >= 0),
        fault_script jsonb NOT NULL DEFAULT '[]'::jsonb
            CHECK (jsonb_typeof(fault_script) = 'array'
                   AND jsonb_array_length(fault_script) <= {MAX_FAULT_RULES}),
        created_at timestamptz NOT NULL DEFAULT clock_timestamp()
    )
    """,
    f"""
    CREATE UNIQUE INDEX IF NOT EXISTS {_T_SCENARIO}_id_uidx
        ON {_T_SCENARIO} (scenario_id)
    """,
    f"""
    CREATE UNIQUE INDEX IF NOT EXISTS {_T_SCENARIO}_nonce_uidx
        ON {_T_SCENARIO} (cursor_nonce)
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {_T_RECORD} (
        scenario_id text NOT NULL
            REFERENCES {_T_SCENARIO}(scenario_id) ON DELETE CASCADE,
        ordinal bigint NOT NULL CHECK (ordinal >= 1),
        record_id text NOT NULL
            CHECK (char_length(record_id) BETWEEN 1 AND {MAX_RECORD_ID_LENGTH})
            CHECK (record_id ~ '{_RECORD_ID_PATTERN}'),
        revision numeric(20, 0) NOT NULL
            CHECK (revision >= 0 AND revision <= {MAX_RECORD_REVISION}),
        content text NOT NULL
            CHECK (octet_length(content) <= {MAX_RECORD_CONTENT_BYTES})
            CHECK (char_length(content) <= {MAX_RECORD_CONTENT_BYTES}),
        PRIMARY KEY (scenario_id, ordinal),
        UNIQUE (scenario_id, record_id)
    )
    """,
    f"""
    CREATE UNIQUE INDEX IF NOT EXISTS {_T_RECORD}_ordinal_uidx
        ON {_T_RECORD} (scenario_id, ordinal)
    """,
    f"""
    CREATE UNIQUE INDEX IF NOT EXISTS {_T_RECORD}_id_uidx
        ON {_T_RECORD} (scenario_id, record_id)
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {_T_SNAPSHOT} (
        scenario_id text NOT NULL
            REFERENCES {_T_SCENARIO}(scenario_id) ON DELETE CASCADE,
        snapshot_id bigint NOT NULL CHECK (snapshot_id >= 1),
        record_count integer NOT NULL CHECK (record_count >= 0),
        created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
        PRIMARY KEY (scenario_id, snapshot_id)
    )
    """,
    f"""
    CREATE UNIQUE INDEX IF NOT EXISTS {_T_SNAPSHOT}_id_uidx
        ON {_T_SNAPSHOT} (scenario_id, snapshot_id)
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {_T_SNAPSHOT_RECORD} (
        scenario_id text NOT NULL,
        snapshot_id bigint NOT NULL,
        ordinal bigint NOT NULL CHECK (ordinal >= 1),
        record_id text NOT NULL
            CHECK (char_length(record_id) BETWEEN 1 AND {MAX_RECORD_ID_LENGTH})
            CHECK (record_id ~ '{_RECORD_ID_PATTERN}'),
        revision numeric(20, 0) NOT NULL
            CHECK (revision >= 0 AND revision <= {MAX_RECORD_REVISION}),
        content text NOT NULL
            CHECK (octet_length(content) <= {MAX_RECORD_CONTENT_BYTES})
            CHECK (char_length(content) <= {MAX_RECORD_CONTENT_BYTES}),
        PRIMARY KEY (scenario_id, snapshot_id, ordinal),
        UNIQUE (scenario_id, snapshot_id, record_id),
        FOREIGN KEY (scenario_id, snapshot_id)
            REFERENCES {_T_SNAPSHOT}(scenario_id, snapshot_id) ON DELETE CASCADE
    )
    """,
    f"""
    CREATE UNIQUE INDEX IF NOT EXISTS {_T_SNAPSHOT_RECORD}_ordinal_uidx
        ON {_T_SNAPSHOT_RECORD} (scenario_id, snapshot_id, ordinal)
    """,
    f"""
    CREATE UNIQUE INDEX IF NOT EXISTS {_T_SNAPSHOT_RECORD}_id_uidx
        ON {_T_SNAPSHOT_RECORD} (scenario_id, snapshot_id, record_id)
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {_T_CURSOR} (
        scenario_id text NOT NULL
            REFERENCES {_T_SCENARIO}(scenario_id) ON DELETE CASCADE,
        token text NOT NULL CHECK (token ~ '^pc1_[0-9a-f]{{16}}_[0-9a-f]{{16}}$'),
        snapshot_id bigint NOT NULL CHECK (snapshot_id >= 1),
        last_record_id text NOT NULL
            CHECK (char_length(last_record_id) BETWEEN 1 AND {MAX_RECORD_ID_LENGTH})
            CHECK (last_record_id ~ '{_RECORD_ID_PATTERN}'),
        last_revision numeric(20, 0) NOT NULL
            CHECK (last_revision >= 0 AND last_revision <= {MAX_RECORD_REVISION}),
        last_used_seq bigint NOT NULL CHECK (last_used_seq >= 1),
        issued_seq bigint NOT NULL CHECK (issued_seq >= 1),
        created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
        PRIMARY KEY (scenario_id, token),
        UNIQUE (scenario_id, snapshot_id, last_record_id, last_revision)
    )
    """,
    f"""
    CREATE UNIQUE INDEX IF NOT EXISTS {_T_CURSOR}_token_uidx
        ON {_T_CURSOR} (scenario_id, token)
    """,
    f"""
    CREATE UNIQUE INDEX IF NOT EXISTS {_T_CURSOR}_position_uidx
        ON {_T_CURSOR} (scenario_id, snapshot_id, last_record_id, last_revision)
    """,
    f"""
    CREATE INDEX IF NOT EXISTS {_T_RECORD}_order_idx
        ON {_T_RECORD} (scenario_id, record_id, revision)
    """,
    f"""
    CREATE INDEX IF NOT EXISTS {_T_SNAPSHOT_RECORD}_order_idx
        ON {_T_SNAPSHOT_RECORD} (scenario_id, snapshot_id, record_id, revision)
    """,
    f"""
    CREATE INDEX IF NOT EXISTS {_T_CURSOR}_lru_idx
        ON {_T_CURSOR} (scenario_id, last_used_seq, token)
    """,
    f"""
    CREATE INDEX IF NOT EXISTS {_T_SNAPSHOT}_lru_idx
        ON {_T_SNAPSHOT} (scenario_id, snapshot_id)
    """,
)

_RECORD_COUNT_SQL: Final[str] = (
    f"SELECT count(*) FROM (SELECT 1 FROM {_T_RECORD} WHERE scenario_id = $1 LIMIT $2) AS bounded"
)
_SNAPSHOT_COUNT_SQL: Final[str] = (
    f"SELECT count(*) FROM (SELECT 1 FROM {_T_SNAPSHOT} WHERE scenario_id = $1 LIMIT $2) AS bounded"
)
_CURSOR_COUNT_SQL: Final[str] = (
    f"SELECT count(*) FROM (SELECT 1 FROM {_T_CURSOR} WHERE scenario_id = $1 LIMIT $2) AS bounded"
)


def _runtime_value(value: object) -> object:
    """Erase static narrowing for deliberate fail-closed runtime validation."""
    return value


def _validated_schema(schema: object) -> str:
    raw = _runtime_value(schema)
    if not isinstance(raw, str) or _SCHEMA_RE.fullmatch(raw) is None:
        raise ProviderContractError(
            "schema must be a lowercase PostgreSQL identifier matching [a-z][a-z0-9_]{0,62}"
        )
    if raw in {"public", "information_schema"} or raw.startswith("pg_"):
        raise ProviderContractError(
            "schema must be a fixture-owned scratch namespace, not a PostgreSQL "
            "system/shared schema"
        )
    return raw


def _validated_scenario(scenario: object) -> str:
    raw = _runtime_value(scenario)
    if not isinstance(raw, str) or re.fullmatch(_SCENARIO_RE, raw) is None:
        raise ProviderContractError("scenario must be 1..128 identifier characters")
    return raw


def _bounded_int(value: int, *, name: str, minimum: int, maximum: int) -> int:
    if type(value) is not int or not minimum <= value <= maximum:
        raise ProviderContractError(f"{name} must be an exact int in [{minimum}, {maximum}]")
    return value


def _validated_nonce(nonce: object) -> str:
    raw = _runtime_value(nonce)
    if not isinstance(raw, str):
        raise ProviderContractError("cursor_nonce must be 16 lowercase hex characters")
    try:
        ProviderCursor.model_validate(f"pc1_{raw}_{0:016x}")
    except ValidationError as exc:
        raise ProviderContractError("cursor_nonce must be 16 lowercase hex characters") from exc
    return raw


def _validated_record(record: object) -> ProviderRecord:
    """Re-validate even a model instance so model_construct cannot smuggle bad data."""
    if not isinstance(record, ProviderRecord):
        raise ProviderMalformedRecord("records must be ProviderRecord instances")
    try:
        checked = ProviderRecord.model_validate(record.model_dump())
    except ValidationError as exc:
        raise ProviderMalformedRecord(f"invalid provider record: {exc}") from exc
    _check_postgres_text(checked)
    return checked


def _record_from_values(record_id: object, revision: object, content: object) -> ProviderRecord:
    try:
        record = ProviderRecord(
            record_id=cast("str", record_id),
            revision=cast("int", revision),
            content=cast("str", content),
        )
    except (ValidationError, TypeError, UnicodeEncodeError) as exc:
        raise ProviderMalformedRecord(f"invalid provider record: {exc}") from exc
    _check_postgres_text(record)
    return record


def _check_postgres_text(record: ProviderRecord) -> None:
    try:
        encoded = record.content.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise ProviderMalformedRecord("content is not valid UTF-8") from exc
    if b"\x00" in encoded:
        raise ProviderMalformedRecord("PostgreSQL text cannot store a NUL byte")


def _revision_from_db(value: object, *, name: str = "revision") -> int:
    raw = _runtime_value(value)
    if isinstance(raw, bool):
        raise ProviderContractError(f"{name} is not an exact integer")
    if isinstance(raw, int):
        result = raw
    elif isinstance(raw, Decimal):
        if not raw.is_finite() or raw != raw.to_integral_value():
            raise ProviderContractError(f"{name} is not an exact integer")
        result = int(raw)
    else:
        raise ProviderContractError(f"{name} is not an exact integer")
    if not 0 <= result <= MAX_RECORD_REVISION:
        raise ProviderContractError(f"{name} is outside the provider revision bound")
    return result


def _exact_int(
    value: object,
    *,
    name: str,
    minimum: int = 0,
    maximum: int | None = None,
) -> int:
    raw = _runtime_value(value)
    if isinstance(raw, bool) or type(raw) is not int:
        raise ProviderContractError(f"{name} is not an exact integer")
    if raw < minimum or (maximum is not None and raw > maximum):
        raise ProviderContractError(f"{name} is outside the declared bound")
    return raw


def _text(value: object, *, name: str) -> str:
    raw = _runtime_value(value)
    if not isinstance(raw, str):
        raise ProviderContractError(f"{name} is not text")
    return raw


_PERSISTED_FAULT_KINDS: Final[frozenset[str]] = frozenset({"unavailable", "rate_limited", "poison"})


def _canonical_json_dumps(payload: object) -> str:
    try:
        return json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise ProviderContractError(
            "fault script contains a non-finite or unsupported JSON value"
        ) from exc


def _fault_script_to_json(script: ProviderFaultScript) -> str:
    payload = [rule.model_dump(mode="json") for rule in script.rules]
    return _canonical_json_dumps(payload)


def _canonical_json(raw: object) -> str:
    if isinstance(raw, str):
        try:
            parsed: object = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ProviderContractError("persisted fault script is not valid JSON") from exc
    elif isinstance(raw, (list, dict)):
        parsed = raw
    else:
        raise ProviderContractError("persisted fault script has an unsupported type")
    return _canonical_json_dumps(parsed)


def _validate_persisted_fault_rule_shape(item: object) -> None:
    """Reject JSON type coercions before the pydantic model can normalize them."""
    if not isinstance(item, dict):
        raise ProviderContractError("persisted fault script contains a non-object rule")
    rule = cast("dict[str, object]", item)
    operation = rule.get("operation")
    if not isinstance(operation, str):
        raise ProviderContractError("persisted fault script rule operation is not text")
    ordinal = rule.get("ordinal")
    if isinstance(ordinal, bool) or type(ordinal) is not int:
        raise ProviderContractError("persisted fault script rule ordinal is not an exact int")
    phase = rule.get("phase")
    if not isinstance(phase, str):
        raise ProviderContractError("persisted fault script rule phase is not text")
    fault = rule.get("fault")
    if not isinstance(fault, dict):
        raise ProviderContractError("persisted fault script rule fault is not an object")
    fault_data = cast("dict[str, object]", fault)
    kind = fault_data.get("kind")
    if not isinstance(kind, str) or kind not in _PERSISTED_FAULT_KINDS:
        raise ProviderContractError("persisted fault script rule kind is unknown")
    message = fault_data.get("message")
    if message is not None and not isinstance(message, str):
        raise ProviderContractError("persisted fault script rule message is not text")
    if kind == "rate_limited":
        delay = fault_data.get("delay_seconds")
        if isinstance(delay, bool) or not isinstance(delay, (int, float)):
            raise ProviderContractError("persisted fault script rate-limit delay is not a number")
        try:
            numeric_delay = float(delay)
        except (OverflowError, ValueError) as exc:
            raise ProviderContractError(
                "persisted fault script rate-limit delay is not a finite number"
            ) from exc
        if not math.isfinite(numeric_delay) or numeric_delay < 0.0:
            raise ProviderContractError(
                "persisted fault script rate-limit delay is not finite nonnegative"
            )


def _fault_script_from_json(canonical: str) -> ProviderFaultScript:
    try:
        payload: object = json.loads(canonical)
    except json.JSONDecodeError as exc:
        raise ProviderContractError("persisted fault script is not valid JSON") from exc
    if not isinstance(payload, list):
        raise ProviderContractError("persisted fault script must be a JSON array")
    rules: list[ProviderFaultRule] = []
    for item in cast("list[object]", payload):
        _validate_persisted_fault_rule_shape(item)
        try:
            rules.append(ProviderFaultRule.model_validate(item))
        except ValidationError as exc:
            raise ProviderContractError("persisted fault script contains an invalid rule") from exc
    try:
        return ProviderFaultScript(rules)
    except ProviderContractError as exc:
        raise ProviderContractError(f"persisted fault script is invalid: {exc}") from exc


def _fault_exception(fault: ProviderFault) -> ProviderError:
    raw = _runtime_value(fault)
    if isinstance(raw, ProviderUnavailableFault):
        return ProviderUnavailable(raw.message)
    if isinstance(raw, ProviderRateLimitedFault):
        return ProviderRateLimited(raw.delay_seconds, raw.message)
    if isinstance(raw, ProviderPoisonFault):
        return ProviderPoison(raw.message)
    raise ProviderContractError(f"unknown provider fault kind: {type(raw).__name__}")


def _validated_cursor_token(cursor: object) -> str:
    raw = _runtime_value(cursor)
    if not isinstance(raw, ProviderCursor):
        raise ProviderUnknownCursor("cursor must be a ProviderCursor")
    token = _runtime_value(raw.root)
    try:
        checked = ProviderCursor.model_validate(token)
    except ValidationError as exc:
        raise ProviderUnknownCursor("cursor is malformed") from exc
    return checked.root


class PgProviderRecordIdentity(BaseModel):
    """Diagnostic provider record identity: stable ordinal plus exact revision."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    scenario: str = Field(min_length=1, max_length=128, pattern=_SCENARIO_RE, strict=True)
    ordinal: int = Field(ge=1, strict=True)
    record_id: str = Field(
        min_length=1,
        max_length=MAX_RECORD_ID_LENGTH,
        pattern=_RECORD_ID_PATTERN,
        strict=True,
    )
    revision: int = Field(ge=0, le=MAX_RECORD_REVISION, strict=True)


class PgProviderScenarioState(BaseModel):
    """Bounded fixture diagnostics for one persisted scenario (not a page)."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    scenario: str = Field(min_length=1, max_length=128, pattern=_SCENARIO_RE, strict=True)
    cursor_nonce: str = Field(pattern=r"^[0-9a-f]{16}$", strict=True)
    max_records: int = Field(ge=1, le=HARD_MAX_RECORDS, strict=True)
    max_page_size: int = Field(ge=1, le=HARD_MAX_PAGE_SIZE, strict=True)
    max_snapshots: int = Field(ge=1, le=HARD_MAX_SNAPSHOTS, strict=True)
    max_cursors: int = Field(ge=1, le=HARD_MAX_CURSORS, strict=True)
    record_count: int = Field(ge=0, strict=True)
    snapshot_count: int = Field(ge=0, strict=True)
    cursor_count: int = Field(ge=0, strict=True)
    fault_rule_count: int = Field(ge=0, le=MAX_FAULT_RULES, strict=True)
    stats: ProviderStats


@dataclass(frozen=True, slots=True)
class _StoredRecord:
    ordinal: int
    record: ProviderRecord


@dataclass(frozen=True, slots=True)
class _PageSlice:
    records: tuple[ProviderRecord, ...]
    has_more: bool
    next_key: tuple[str, int] | None

    @property
    def payload_bytes(self) -> int:
        return sum(record.payload_bytes for record in self.records)


@dataclass(frozen=True, slots=True)
class _CursorRow:
    snapshot_id: int
    last_record_id: str
    last_revision: int


@dataclass(frozen=True, slots=True)
class _ScenarioRow:
    scenario: str
    cursor_nonce: str
    max_records: int
    max_page_size: int
    max_snapshots: int
    max_cursors: int
    next_ordinal: int
    next_snapshot_id: int
    next_cursor_serial: int
    next_use_serial: int
    record_count: int
    snapshot_count: int
    cursor_count: int
    fetch_calls: int
    fault_gates_tripped: int
    pages_returned: int
    records_returned: int
    payload_bytes: int
    withheld_pages: int
    withheld_payload_bytes: int
    cursors_issued: int
    snapshots_created: int
    fault_script: ProviderFaultScript


def _stored_from_row(row: asyncpg.Record) -> _StoredRecord:
    return _StoredRecord(
        ordinal=_exact_int(row["ordinal"], name="record ordinal", minimum=1),
        record=_record_from_values(
            row["record_id"],
            _revision_from_db(row["revision"]),
            row["content"],
        ),
    )


def _scenario_from_row(row: asyncpg.Record) -> _ScenarioRow:
    scenario = _validated_scenario(_text(row["scenario_id"], name="scenario_id"))
    nonce = _text(row["cursor_nonce"], name="cursor_nonce")
    _validated_nonce(nonce)
    return _ScenarioRow(
        scenario=scenario,
        cursor_nonce=nonce,
        max_records=_exact_int(
            row["max_records"], name="max_records", minimum=1, maximum=HARD_MAX_RECORDS
        ),
        max_page_size=_exact_int(
            row["max_page_size"], name="max_page_size", minimum=1, maximum=HARD_MAX_PAGE_SIZE
        ),
        max_snapshots=_exact_int(
            row["max_snapshots"], name="max_snapshots", minimum=1, maximum=HARD_MAX_SNAPSHOTS
        ),
        max_cursors=_exact_int(
            row["max_cursors"], name="max_cursors", minimum=1, maximum=HARD_MAX_CURSORS
        ),
        next_ordinal=_exact_int(row["next_ordinal"], name="next_ordinal", minimum=1),
        next_snapshot_id=_exact_int(row["next_snapshot_id"], name="next_snapshot_id", minimum=1),
        next_cursor_serial=_exact_int(
            row["next_cursor_serial"], name="next_cursor_serial", minimum=1
        ),
        next_use_serial=_exact_int(row["next_use_serial"], name="next_use_serial", minimum=1),
        record_count=_exact_int(row["record_count"], name="record_count"),
        snapshot_count=_exact_int(row["snapshot_count"], name="snapshot_count"),
        cursor_count=_exact_int(row["cursor_count"], name="cursor_count"),
        fetch_calls=_exact_int(row["fetch_calls"], name="fetch_calls"),
        fault_gates_tripped=_exact_int(row["fault_gates_tripped"], name="fault_gates_tripped"),
        pages_returned=_exact_int(row["pages_returned"], name="pages_returned"),
        records_returned=_exact_int(row["records_returned"], name="records_returned"),
        payload_bytes=_exact_int(row["payload_bytes"], name="payload_bytes"),
        withheld_pages=_exact_int(row["withheld_pages"], name="withheld_pages"),
        withheld_payload_bytes=_exact_int(
            row["withheld_payload_bytes"], name="withheld_payload_bytes"
        ),
        cursors_issued=_exact_int(row["cursors_issued"], name="cursors_issued"),
        snapshots_created=_exact_int(row["snapshots_created"], name="snapshots_created"),
        fault_script=_fault_script_from_json(_canonical_json(row["fault_script"])),
    )


def _stats_from_scenario(scenario: _ScenarioRow) -> ProviderStats:
    return ProviderStats(
        fetch_calls=scenario.fetch_calls,
        fault_gates_tripped=scenario.fault_gates_tripped,
        pages_returned=scenario.pages_returned,
        records_returned=scenario.records_returned,
        payload_bytes=scenario.payload_bytes,
        withheld_pages=scenario.withheld_pages,
        withheld_payload_bytes=scenario.withheld_payload_bytes,
        cursors_issued=scenario.cursors_issued,
        snapshots_created=scenario.snapshots_created,
    )


def _scenario_state(scenario: _ScenarioRow) -> PgProviderScenarioState:
    return PgProviderScenarioState(
        scenario=scenario.scenario,
        cursor_nonce=scenario.cursor_nonce,
        max_records=scenario.max_records,
        max_page_size=scenario.max_page_size,
        max_snapshots=scenario.max_snapshots,
        max_cursors=scenario.max_cursors,
        record_count=scenario.record_count,
        snapshot_count=scenario.snapshot_count,
        cursor_count=scenario.cursor_count,
        fault_rule_count=len(scenario.fault_script.rules),
        stats=_stats_from_scenario(scenario),
    )


def _slice_page(stored: Sequence[_StoredRecord], page_limit: int) -> _PageSlice:
    has_more = len(stored) > page_limit
    page_records = tuple(item.record for item in stored[:page_limit])
    next_key = page_records[-1].key if has_more else None
    return _PageSlice(records=page_records, has_more=has_more, next_key=next_key)


class PgPagedProvider:
    """Bounded shared paged provider implemented on a real asyncpg pool.

    The pool is caller-owned and never closed by this fixture. Constructing a
    provider performs no I/O; ``initialize()`` is the explicit scratch-schema
    setup step and is idempotent. All provider instances for the same
    ``scenario`` in the same ``schema`` share durable snapshot/cursor/fault
    state while never sharing an in-memory registry.
    """

    def __init__(
        self,
        pool: asyncpg.Pool,
        *,
        schema: str,
        scenario: str = "default",
        max_records: int = DEFAULT_MAX_RECORDS,
        max_page_size: int = DEFAULT_MAX_PAGE_SIZE,
        max_snapshots: int = DEFAULT_MAX_SNAPSHOTS,
        max_cursors: int = DEFAULT_MAX_CURSORS,
        cursor_nonce: str | None = None,
        fault_script: ProviderFaultScript | None = None,
    ) -> None:
        self._pool = pool
        self._schema = _validated_schema(schema)
        self._scenario = _validated_scenario(scenario)
        self._max_records = _bounded_int(
            max_records, name="max_records", minimum=1, maximum=HARD_MAX_RECORDS
        )
        self._max_page_size = _bounded_int(
            max_page_size, name="max_page_size", minimum=1, maximum=HARD_MAX_PAGE_SIZE
        )
        self._max_snapshots = _bounded_int(
            max_snapshots, name="max_snapshots", minimum=1, maximum=HARD_MAX_SNAPSHOTS
        )
        self._max_cursors = _bounded_int(
            max_cursors, name="max_cursors", minimum=1, maximum=HARD_MAX_CURSORS
        )
        self._requested_nonce = None if cursor_nonce is None else _validated_nonce(cursor_nonce)
        raw_script = _runtime_value(fault_script)
        if raw_script is None:
            self._configured_script = ProviderFaultScript()
            self._script_supplied = False
        elif isinstance(raw_script, ProviderFaultScript):
            self._configured_script = raw_script
            self._script_supplied = True
        else:
            raise ProviderContractError("fault_script must be a ProviderFaultScript")
        self._fault_script = self._configured_script

    @property
    def schema(self) -> str:
        return self._schema

    @property
    def scenario(self) -> str:
        return self._scenario

    @property
    def fault_script(self) -> ProviderFaultScript:
        """The effective script (persisted rules after ``initialize``)."""
        return self._fault_script

    async def initialize(self) -> None:
        """Create/validate only this fixture's deterministic scratch tables.

        The idempotent DDL runs in its own short transaction under a
        PostgreSQL advisory transaction lock keyed by the validated fixture
        schema. The scenario row transaction is separate, so DDL locks are
        never held while scenario DML runs. Table creation is skipped when
        the complete fixture shape and indexes already exist. Any unexpected
        server/client driver failure is translated to the fixture's typed
        contract family with the original exception preserved as ``__cause__``.
        """
        try:
            await self._initialize_schema()
            await self._initialize_scenario()
        except ProviderContractError:
            raise
        except (asyncpg.PostgresError, asyncpg.InterfaceError) as exc:
            raise ProviderContractError(
                f"provider fixture initialization failed for schema {self._schema!r}: "
                f"{type(exc).__name__}: {exc}"
            ) from exc

    async def _initialize_schema(self) -> None:
        """Serialize only short bootstrap DDL; never mix it with scenario DML."""
        async with self._pool.acquire() as conn, conn.transaction():
            # This advisory lock only serializes short fixture bootstrap DDL
            # for the same validated schema. The hash is a lock domain, never
            # a record/scenario/cursor identity.
            await conn.execute(
                "SELECT pg_advisory_xact_lock(hashtext($1)::bigint)",
                f"examples.adoption.pg_provider.initialize:{self._schema}",
            )
            try:
                await conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{self._schema}"')
            except asyncpg.IntegrityConstraintViolationError as exc:
                raise ProviderContractError(
                    f"fixture schema {self._schema!r} bootstrap raced another creator: "
                    f"{type(exc).__name__}"
                ) from exc
            await self._set_search_path(conn)
            await self._validate_table_shapes(conn, existing_only=True)
            if await self._fixture_schema_current(conn):
                return
            for statement in _DDL_STATEMENTS:
                try:
                    await conn.execute(statement)
                except asyncpg.IntegrityConstraintViolationError as exc:
                    raise ProviderContractError(
                        f"fixture schema {self._schema!r} has pre-existing rows or "
                        "constraints incompatible with its deterministic shape"
                    ) from exc
            await self._validate_table_shapes(conn)

    async def _initialize_scenario(self) -> None:
        async with self._pool.acquire() as conn, conn.transaction():
            await self._set_search_path(conn)
            row = await conn.fetchrow(
                f"SELECT * FROM {_T_SCENARIO} WHERE scenario_id = $1 FOR UPDATE",
                self._scenario,
            )
            if row is None:
                nonce = self._requested_nonce or secrets.token_hex(8)
                conflicting = await conn.fetchval(
                    f"SELECT scenario_id FROM {_T_SCENARIO} "
                    "WHERE cursor_nonce = $1 AND scenario_id <> $2",
                    nonce,
                    self._scenario,
                )
                if conflicting is not None:
                    raise ProviderContractError(
                        f"cursor nonce {nonce!r} is already persisted for scenario "
                        f"{conflicting!r}; distinct scenarios must not share a nonce"
                    )
                try:
                    await conn.execute(
                        f"""
                        INSERT INTO {_T_SCENARIO}
                            (scenario_id, cursor_nonce, max_records, max_page_size,
                             max_snapshots, max_cursors, fault_script)
                        VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
                        ON CONFLICT (scenario_id) DO NOTHING
                        """,
                        self._scenario,
                        nonce,
                        self._max_records,
                        self._max_page_size,
                        self._max_snapshots,
                        self._max_cursors,
                        _fault_script_to_json(self._configured_script),
                    )
                except asyncpg.UniqueViolationError as exc:
                    raise ProviderContractError(
                        f"cursor nonce {nonce!r} is already persisted for another scenario"
                    ) from exc
                row = await conn.fetchrow(
                    f"SELECT * FROM {_T_SCENARIO} WHERE scenario_id = $1 FOR UPDATE",
                    self._scenario,
                )
            if row is None:
                raise ProviderContractError(f"scenario {self._scenario!r} could not be initialized")
            scenario = _scenario_from_row(row)
            self._validate_scenario_match(scenario)
            conflicting = await conn.fetchval(
                f"SELECT scenario_id FROM {_T_SCENARIO} "
                "WHERE cursor_nonce = $1 AND scenario_id <> $2 LIMIT 1",
                scenario.cursor_nonce,
                scenario.scenario,
            )
            if conflicting is not None:
                raise ProviderContractError(
                    f"cursor nonce {scenario.cursor_nonce!r} is also persisted for "
                    f"scenario {conflicting!r}; the fixture schema is ambiguous"
                )
            await self._validate_retained_counts(conn, scenario)
            self._fault_script = scenario.fault_script

    async def _set_search_path(self, conn: asyncpg.Connection) -> None:
        await conn.execute("SELECT set_config('search_path', $1, true)", self._schema)

    async def _read_schema_shape(
        self, conn: asyncpg.Connection
    ) -> tuple[dict[str, set[str]], dict[str, dict[str, str]], set[str]]:
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
        return present, types, seen

    async def _validate_table_shapes(
        self, conn: asyncpg.Connection, *, existing_only: bool = False
    ) -> None:
        present, types, seen = await self._read_schema_shape(conn)
        missing = [
            f"{table}.{column}"
            for table in sorted(_EXPECTED_COLUMNS)
            if not existing_only or table in seen
            for column in sorted(_EXPECTED_COLUMNS[table] - present[table])
        ]
        if missing:
            shown = ", ".join(missing[:12])
            suffix = "" if len(missing) <= 12 else f" (+{len(missing) - 12} more)"
            raise ProviderContractError(
                f"fixture schema {self._schema!r} has incompatible pre-existing tables "
                f"(missing columns: {shown}{suffix}); use a fresh scratch schema or drop "
                "the fixture's own tables. This fixture never alters an existing table shape."
            )
        wrong_types = [
            f"{table}.{column} expected {expected} got {types[table].get(column, 'unknown')}"
            for table in sorted(_EXPECTED_TYPES)
            if not existing_only or table in seen
            for column, expected in sorted(_EXPECTED_TYPES[table].items())
            if types[table].get(column) != expected
        ]
        if wrong_types:
            shown = ", ".join(wrong_types[:12])
            suffix = "" if len(wrong_types) <= 12 else f" (+{len(wrong_types) - 12} more)"
            raise ProviderContractError(
                f"fixture schema {self._schema!r} has incompatible pre-existing column "
                f"types: {shown}; use a fresh scratch schema or drop the fixture's own tables."
            )

    async def _fixture_schema_current(self, conn: asyncpg.Connection) -> bool:
        present, types, seen = await self._read_schema_shape(conn)
        if seen != set(_EXPECTED_COLUMNS):
            return False
        for table in _EXPECTED_COLUMNS:
            if not _EXPECTED_COLUMNS[table].issubset(present[table]):
                return False
            for column, expected in _EXPECTED_TYPES[table].items():
                if types[table].get(column) != expected:
                    return False
        index_rows = await conn.fetch(
            "SELECT indexname FROM pg_indexes "
            "WHERE schemaname = $1 AND indexname = ANY($2::text[])",
            self._schema,
            list(_REQUIRED_INDEX_NAMES),
        )
        indexes = {row["indexname"] for row in index_rows if isinstance(row["indexname"], str)}
        return indexes == set(_REQUIRED_INDEX_NAMES)

    def _validate_scenario_match(self, scenario: _ScenarioRow) -> None:
        expected = (
            (scenario.max_records, self._max_records, "max_records"),
            (scenario.max_page_size, self._max_page_size, "max_page_size"),
            (scenario.max_snapshots, self._max_snapshots, "max_snapshots"),
            (scenario.max_cursors, self._max_cursors, "max_cursors"),
        )
        for stored, requested, name in expected:
            if stored != requested:
                raise ProviderContractError(
                    f"scenario {self._scenario!r} is persisted with {name}={stored}, "
                    f"not {requested}"
                )
        if self._requested_nonce is not None and scenario.cursor_nonce != self._requested_nonce:
            raise ProviderContractError(
                f"scenario {self._scenario!r} is persisted with a different cursor nonce"
            )
        if self._script_supplied and self._configured_script.rules != scenario.fault_script.rules:
            raise ProviderContractError(
                f"scenario {self._scenario!r} is persisted with a different fault script"
            )

    async def _validate_retained_counts(
        self, conn: asyncpg.Connection, scenario: _ScenarioRow
    ) -> None:
        record_count = _exact_int(
            await conn.fetchval(_RECORD_COUNT_SQL, scenario.scenario, scenario.max_records + 1),
            name="bounded record count",
        )
        if record_count > scenario.max_records:
            raise ProviderRecordLimitExceeded(
                f"scenario {scenario.scenario!r} exceeds max_records={scenario.max_records}"
            )
        if record_count != scenario.record_count:
            raise ProviderContractError(
                f"scenario {scenario.scenario!r} record_count does not match its rows"
            )
        snapshot_count = _exact_int(
            await conn.fetchval(_SNAPSHOT_COUNT_SQL, scenario.scenario, scenario.max_snapshots + 1),
            name="bounded snapshot count",
        )
        if snapshot_count > scenario.max_snapshots or snapshot_count != scenario.snapshot_count:
            raise ProviderContractError(
                f"scenario {scenario.scenario!r} snapshot retention is inconsistent"
            )
        cursor_count = _exact_int(
            await conn.fetchval(_CURSOR_COUNT_SQL, scenario.scenario, scenario.max_cursors + 1),
            name="bounded cursor count",
        )
        if cursor_count > scenario.max_cursors or cursor_count != scenario.cursor_count:
            raise ProviderContractError(
                f"scenario {scenario.scenario!r} cursor retention is inconsistent"
            )

    async def _load_scenario(
        self, conn: asyncpg.Connection, *, for_update: bool = False
    ) -> _ScenarioRow:
        suffix = " FOR UPDATE" if for_update else ""
        row = await conn.fetchrow(
            f"SELECT * FROM {_T_SCENARIO} WHERE scenario_id = $1{suffix}",
            self._scenario,
        )
        if row is None:
            raise ProviderContractError(
                f"scenario {self._scenario!r} is not initialized; call initialize()"
            )
        return _scenario_from_row(row)

    async def read_scenario_state(self) -> PgProviderScenarioState:
        async with (
            self._pool.acquire() as conn,
            conn.transaction(isolation="repeatable_read", readonly=True),
        ):
            await self._set_search_path(conn)
            return _scenario_state(await self._load_scenario(conn))

    async def read_stats(self) -> ProviderStats:
        return (await self.read_scenario_state()).stats

    async def current_records(self) -> tuple[ProviderRecord, ...]:
        async with (
            self._pool.acquire() as conn,
            conn.transaction(isolation="repeatable_read", readonly=True),
        ):
            await self._set_search_path(conn)
            scenario = await self._load_scenario(conn)
            stored = await self._read_source_records(conn, scenario)
            return tuple(item.record for item in stored)

    async def record_identities(self) -> tuple[PgProviderRecordIdentity, ...]:
        async with (
            self._pool.acquire() as conn,
            conn.transaction(isolation="repeatable_read", readonly=True),
        ):
            await self._set_search_path(conn)
            scenario = await self._load_scenario(conn)
            stored = await self._read_source_records(conn, scenario)
            return tuple(
                PgProviderRecordIdentity(
                    scenario=scenario.scenario,
                    ordinal=item.ordinal,
                    record_id=item.record.record_id,
                    revision=item.record.revision,
                )
                for item in stored
            )

    async def snapshot_record_identities(
        self, cursor: ProviderCursor
    ) -> tuple[PgProviderRecordIdentity, ...]:
        """Diagnostic view of one retained snapshot's record identities."""
        async with (
            self._pool.acquire() as conn,
            conn.transaction(isolation="repeatable_read", readonly=True),
        ):
            await self._set_search_path(conn)
            scenario = await self._load_scenario(conn)
            token = _validated_cursor_token(cursor)
            self._check_cursor_nonce(scenario, token)
            cursor_row = await self._read_cursor(conn, scenario, token)
            if cursor_row is None:
                raise ProviderUnknownCursor("cursor was not issued by this scenario")
            if not await self._snapshot_exists(conn, scenario, cursor_row.snapshot_id):
                raise ProviderCursorExpired("cursor's snapshot has been released")
            stored = await self._read_all_snapshot_records(conn, scenario, cursor_row.snapshot_id)
            return tuple(
                PgProviderRecordIdentity(
                    scenario=scenario.scenario,
                    ordinal=item.ordinal,
                    record_id=item.record.record_id,
                    revision=item.record.revision,
                )
                for item in stored
            )

    async def replace_records(self, records: Iterable[ProviderRecord]) -> None:
        """Atomically replace the bounded source and invalidate old cursors."""
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

        async with self._pool.acquire() as conn, conn.transaction():
            await self._set_search_path(conn)
            scenario = await self._load_scenario(conn, for_update=True)
            await conn.execute(
                f"DELETE FROM {_T_SNAPSHOT_RECORD} WHERE scenario_id = $1", scenario.scenario
            )
            await conn.execute(
                f"DELETE FROM {_T_SNAPSHOT} WHERE scenario_id = $1", scenario.scenario
            )
            await conn.execute(f"DELETE FROM {_T_CURSOR} WHERE scenario_id = $1", scenario.scenario)
            await conn.execute(f"DELETE FROM {_T_RECORD} WHERE scenario_id = $1", scenario.scenario)
            if materialized:
                await conn.executemany(
                    f"""
                        INSERT INTO {_T_RECORD}
                            (scenario_id, ordinal, record_id, revision, content)
                        VALUES ($1, $2, $3, $4::numeric, $5)
                        """,
                    [
                        (
                            scenario.scenario,
                            ordinal,
                            record.record_id,
                            record.revision,
                            record.content,
                        )
                        for ordinal, record in enumerate(materialized, start=1)
                    ],
                )
            await conn.execute(
                f"""
                    UPDATE {_T_SCENARIO}
                    SET record_count = $2,
                        next_ordinal = $3,
                        snapshot_count = 0,
                        cursor_count = 0
                    WHERE scenario_id = $1
                    """,
                scenario.scenario,
                len(materialized),
                len(materialized) + 1,
            )

    async def upsert(
        self,
        record_id: str,
        content: str,
        *,
        revision: int | None = None,
    ) -> ProviderRecord:
        """Insert or revise one record; revisions are monotonic and fail closed."""
        raw_record_id = _runtime_value(record_id)
        if not isinstance(raw_record_id, str):
            raise ProviderMalformedRecord("record_id must be text")
        record_id = raw_record_id
        if revision is not None and type(revision) is not int:
            raise ProviderMalformedRecord("revision must be an exact nonnegative int")
        async with self._pool.acquire() as conn, conn.transaction():
            await self._set_search_path(conn)
            scenario = await self._load_scenario(conn, for_update=True)
            row = await conn.fetchrow(
                f"""
                    SELECT ordinal, record_id, revision, content
                    FROM {_T_RECORD}
                    WHERE scenario_id = $1 AND record_id = $2
                    """,
                scenario.scenario,
                record_id,
            )
            if row is None:
                if scenario.record_count >= scenario.max_records:
                    raise ProviderRecordLimitExceeded(
                        f"source bound of {scenario.max_records} records would be exceeded"
                    )
                next_revision = 0 if revision is None else revision
                record = _record_from_values(record_id, next_revision, content)
                ordinal = scenario.next_ordinal
                await conn.execute(
                    f"""
                        INSERT INTO {_T_RECORD}
                            (scenario_id, ordinal, record_id, revision, content)
                        VALUES ($1, $2, $3, $4::numeric, $5)
                        """,
                    scenario.scenario,
                    ordinal,
                    record.record_id,
                    record.revision,
                    record.content,
                )
                await conn.execute(
                    f"""
                        UPDATE {_T_SCENARIO}
                        SET next_ordinal = next_ordinal + 1,
                            record_count = record_count + 1
                        WHERE scenario_id = $1
                        """,
                    scenario.scenario,
                )
                return record

            existing = _record_from_values(
                row["record_id"], _revision_from_db(row["revision"]), row["content"]
            )
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
            record = _record_from_values(existing.record_id, next_revision, content)
            await conn.execute(
                f"""
                    UPDATE {_T_RECORD}
                    SET revision = $3::numeric, content = $4
                    WHERE scenario_id = $1 AND record_id = $2
                    """,
                scenario.scenario,
                record.record_id,
                record.revision,
                record.content,
            )
            return record

    async def fetch(self, cursor: ProviderCursor | None, page_limit: int) -> ProviderPage:
        async with self._pool.acquire() as conn:
            transaction = conn.transaction()
            await transaction.start()
            pending: ProviderError | None = None
            page: ProviderPage | None = None
            try:
                await self._set_search_path(conn)
                scenario = await self._load_scenario(conn, for_update=True)
                ordinal = scenario.fetch_calls + 1
                await conn.execute(
                    f"UPDATE {_T_SCENARIO} SET fetch_calls = $2 WHERE scenario_id = $1",
                    scenario.scenario,
                    ordinal,
                )

                if type(page_limit) is not int or not 1 <= page_limit <= scenario.max_page_size:
                    pending = ProviderPageLimitError(
                        f"page_limit must be an exact int in [1, {scenario.max_page_size}]"
                    )
                else:
                    raw_cursor = _runtime_value(cursor)
                    if raw_cursor is None:
                        pending, page = await self._fetch_first_page(
                            conn, scenario, ordinal, page_limit
                        )
                    else:
                        try:
                            token = _validated_cursor_token(raw_cursor)
                            self._check_cursor_nonce(scenario, token)
                            cursor_row = await self._read_cursor(conn, scenario, token)
                            if cursor_row is None:
                                raise ProviderUnknownCursor(
                                    "cursor was not issued by this scenario"
                                )
                            if not await self._snapshot_exists(
                                conn, scenario, cursor_row.snapshot_id
                            ):
                                raise ProviderCursorExpired("cursor's snapshot has been released")
                        except (ProviderUnknownCursor, ProviderCursorExpired) as exc:
                            pending = exc
                        else:
                            pending, page = await self._fetch_cursor_page(
                                conn,
                                scenario,
                                ordinal,
                                page_limit,
                                token,
                                cursor_row,
                            )
            except BaseException:
                await transaction.rollback()
                raise
            await transaction.commit()
            if pending is not None:
                raise pending
            if page is None:
                raise ProviderContractError("fetch produced neither a page nor a typed failure")
            return page

    async def _fetch_first_page(
        self,
        conn: asyncpg.Connection,
        scenario: _ScenarioRow,
        ordinal: int,
        page_limit: int,
    ) -> tuple[ProviderError | None, ProviderPage | None]:
        rule = self._fault_script.rule_for(ordinal)
        if rule is not None and rule.phase == "before_page":
            await self._bump_fault(conn, scenario.scenario, withheld_pages=0, withheld_bytes=0)
            return _fault_exception(rule.fault), None

        stored = await self._read_source_records(conn, scenario)
        page_slice = _slice_page(stored, page_limit)
        if rule is not None:
            await self._bump_fault(
                conn,
                scenario.scenario,
                withheld_pages=1,
                withheld_bytes=page_slice.payload_bytes,
            )
            return _fault_exception(rule.fault), None

        next_cursor: ProviderCursor | None = None
        if page_slice.has_more:
            snapshot_id = scenario.next_snapshot_id
            await self._store_snapshot(conn, scenario, snapshot_id)
            if page_slice.next_key is None:
                raise ProviderContractError("nonterminal page has no continuation key")
            next_cursor = await self._issue_or_get_cursor(
                conn, scenario, snapshot_id, page_slice.next_key
            )
        page = ProviderPage(
            records=page_slice.records,
            next_cursor=next_cursor,
            has_more=page_slice.has_more,
        )
        await self._bump_success(conn, scenario.scenario, len(page.records), page.payload_bytes)
        return None, page

    async def _fetch_cursor_page(
        self,
        conn: asyncpg.Connection,
        scenario: _ScenarioRow,
        ordinal: int,
        page_limit: int,
        token: str,
        cursor_row: _CursorRow,
    ) -> tuple[ProviderError | None, ProviderPage | None]:
        rule = self._fault_script.rule_for(ordinal)
        if rule is not None and rule.phase == "before_page":
            await self._bump_fault(conn, scenario.scenario, withheld_pages=0, withheld_bytes=0)
            return _fault_exception(rule.fault), None

        stored = await self._read_snapshot_page(conn, scenario, cursor_row, page_limit)
        page_slice = _slice_page(stored, page_limit)
        if rule is not None:
            await self._bump_fault(
                conn,
                scenario.scenario,
                withheld_pages=1,
                withheld_bytes=page_slice.payload_bytes,
            )
            return _fault_exception(rule.fault), None

        await self._touch_cursor(conn, scenario.scenario, token)
        next_cursor: ProviderCursor | None = None
        if page_slice.has_more:
            if page_slice.next_key is None:
                raise ProviderContractError("nonterminal page has no continuation key")
            next_cursor = await self._issue_or_get_cursor(
                conn, scenario, cursor_row.snapshot_id, page_slice.next_key
            )
        page = ProviderPage(
            records=page_slice.records,
            next_cursor=next_cursor,
            has_more=page_slice.has_more,
        )
        await self._bump_success(conn, scenario.scenario, len(page.records), page.payload_bytes)
        return None, page

    def _check_cursor_nonce(self, scenario: _ScenarioRow, token: str) -> None:
        if token[4:20] != scenario.cursor_nonce:
            raise ProviderUnknownCursor("cursor was not issued by this scenario")

    async def _read_source_records(
        self, conn: asyncpg.Connection, scenario: _ScenarioRow
    ) -> tuple[_StoredRecord, ...]:
        rows = await conn.fetch(
            f"""
            SELECT ordinal, record_id, revision, content
            FROM {_T_RECORD}
            WHERE scenario_id = $1
            ORDER BY record_id, revision
            LIMIT $2
            """,
            scenario.scenario,
            scenario.max_records + 1,
        )
        if len(rows) > scenario.max_records:
            raise ProviderRecordLimitExceeded(
                f"source exceeds the bound of {scenario.max_records} records"
            )
        stored = tuple(_stored_from_row(row) for row in rows)
        if len(stored) != scenario.record_count:
            raise ProviderContractError(
                f"scenario {scenario.scenario!r} record_count does not match its rows"
            )
        ordinals = {item.ordinal for item in stored}
        if len(ordinals) != len(stored):
            raise ProviderContractError("source contains duplicate record ordinals")
        return stored

    async def _read_snapshot_page(
        self,
        conn: asyncpg.Connection,
        scenario: _ScenarioRow,
        cursor_row: _CursorRow,
        page_limit: int,
    ) -> tuple[_StoredRecord, ...]:
        rows = await conn.fetch(
            f"""
            SELECT ordinal, record_id, revision, content
            FROM {_T_SNAPSHOT_RECORD}
            WHERE scenario_id = $1
              AND snapshot_id = $2
              AND (record_id, revision) > ($3::text, $4::numeric)
            ORDER BY record_id, revision
            LIMIT $5
            """,
            scenario.scenario,
            cursor_row.snapshot_id,
            cursor_row.last_record_id,
            cursor_row.last_revision,
            page_limit + 1,
        )
        stored = tuple(_stored_from_row(row) for row in rows)
        ordinals = {item.ordinal for item in stored}
        if len(ordinals) != len(stored):
            raise ProviderContractError("snapshot page contains duplicate record ordinals")
        return stored

    async def _read_all_snapshot_records(
        self,
        conn: asyncpg.Connection,
        scenario: _ScenarioRow,
        snapshot_id: int,
    ) -> tuple[_StoredRecord, ...]:
        rows = await conn.fetch(
            f"""
            SELECT ordinal, record_id, revision, content
            FROM {_T_SNAPSHOT_RECORD}
            WHERE scenario_id = $1 AND snapshot_id = $2
            ORDER BY record_id, revision
            LIMIT $3
            """,
            scenario.scenario,
            snapshot_id,
            scenario.max_records + 1,
        )
        if len(rows) > scenario.max_records:
            raise ProviderRecordLimitExceeded("snapshot exceeds the declared record bound")
        return tuple(_stored_from_row(row) for row in rows)

    async def _read_cursor(
        self,
        conn: asyncpg.Connection,
        scenario: _ScenarioRow,
        token: str,
    ) -> _CursorRow | None:
        row = await conn.fetchrow(
            f"""
            SELECT snapshot_id, last_record_id, last_revision
            FROM {_T_CURSOR}
            WHERE scenario_id = $1 AND token = $2
            """,
            scenario.scenario,
            token,
        )
        if row is None:
            return None
        last_record = _record_from_values(
            row["last_record_id"], _revision_from_db(row["last_revision"]), ""
        )
        return _CursorRow(
            snapshot_id=_exact_int(row["snapshot_id"], name="cursor snapshot_id", minimum=1),
            last_record_id=last_record.record_id,
            last_revision=last_record.revision,
        )

    async def _snapshot_exists(
        self,
        conn: asyncpg.Connection,
        scenario: _ScenarioRow,
        snapshot_id: int,
    ) -> bool:
        found = await conn.fetchval(
            f"SELECT 1 FROM {_T_SNAPSHOT} WHERE scenario_id = $1 AND snapshot_id = $2",
            scenario.scenario,
            snapshot_id,
        )
        return found is not None

    async def _store_snapshot(
        self,
        conn: asyncpg.Connection,
        scenario: _ScenarioRow,
        snapshot_id: int,
    ) -> None:
        if scenario.snapshot_count >= scenario.max_snapshots:
            excess = scenario.snapshot_count - scenario.max_snapshots + 1
            await conn.execute(
                f"""
                DELETE FROM {_T_SNAPSHOT_RECORD}
                WHERE scenario_id = $1
                  AND snapshot_id IN (
                      SELECT snapshot_id
                      FROM {_T_SNAPSHOT}
                      WHERE scenario_id = $1
                      ORDER BY snapshot_id ASC
                      LIMIT $2
                  )
                """,
                scenario.scenario,
                excess,
            )
            await conn.execute(
                f"""
                DELETE FROM {_T_SNAPSHOT}
                WHERE scenario_id = $1
                  AND snapshot_id IN (
                      SELECT snapshot_id
                      FROM {_T_SNAPSHOT}
                      WHERE scenario_id = $1
                      ORDER BY snapshot_id ASC
                      LIMIT $2
                  )
                """,
                scenario.scenario,
                excess,
            )
            await conn.execute(
                f"""
                UPDATE {_T_SCENARIO}
                SET snapshot_count = snapshot_count - $2
                WHERE scenario_id = $1
                """,
                scenario.scenario,
                excess,
            )
        await conn.execute(
            f"""
            INSERT INTO {_T_SNAPSHOT} (scenario_id, snapshot_id, record_count)
            VALUES ($1, $2, $3)
            """,
            scenario.scenario,
            snapshot_id,
            scenario.record_count,
        )
        await conn.execute(
            f"""
            INSERT INTO {_T_SNAPSHOT_RECORD}
                (scenario_id, snapshot_id, ordinal, record_id, revision, content)
            SELECT scenario_id, $2, ordinal, record_id, revision, content
            FROM {_T_RECORD}
            WHERE scenario_id = $1
            """,
            scenario.scenario,
            snapshot_id,
        )
        count = _exact_int(
            await conn.fetchval(
                f"""
                UPDATE {_T_SCENARIO}
                SET next_snapshot_id = next_snapshot_id + 1,
                    snapshot_count = snapshot_count + 1,
                    snapshots_created = snapshots_created + 1
                WHERE scenario_id = $1
                RETURNING snapshot_count
                """,
                scenario.scenario,
            ),
            name="snapshot_count",
            minimum=0,
        )
        if count > scenario.max_snapshots:
            raise ProviderContractError("snapshot retention exceeded its declared bound")

    async def _next_use_serial(self, conn: asyncpg.Connection, scenario: str) -> int:
        value = await conn.fetchval(
            f"""
            UPDATE {_T_SCENARIO}
            SET next_use_serial = next_use_serial + 1
            WHERE scenario_id = $1
            RETURNING next_use_serial - 1
            """,
            scenario,
        )
        return _exact_int(value, name="cursor use serial", minimum=1)

    async def _touch_cursor(self, conn: asyncpg.Connection, scenario: str, token: str) -> None:
        use_serial = await self._next_use_serial(conn, scenario)
        await conn.execute(
            f"""
            UPDATE {_T_CURSOR}
            SET last_used_seq = $3
            WHERE scenario_id = $1 AND token = $2
            """,
            scenario,
            token,
            use_serial,
        )

    async def _issue_or_get_cursor(
        self,
        conn: asyncpg.Connection,
        scenario: _ScenarioRow,
        snapshot_id: int,
        last_key: tuple[str, int],
    ) -> ProviderCursor:
        last_record_id, last_revision = last_key
        existing = await conn.fetchrow(
            f"""
            SELECT token
            FROM {_T_CURSOR}
            WHERE scenario_id = $1
              AND snapshot_id = $2
              AND last_record_id = $3
              AND last_revision = $4::numeric
            """,
            scenario.scenario,
            snapshot_id,
            last_record_id,
            last_revision,
        )
        if existing is not None:
            token = _text(existing["token"], name="cursor token")
            use_serial = await self._next_use_serial(conn, scenario.scenario)
            await conn.execute(
                f"""
                UPDATE {_T_CURSOR}
                SET last_used_seq = $3
                WHERE scenario_id = $1 AND token = $2
                """,
                scenario.scenario,
                token,
                use_serial,
            )
            return ProviderCursor(token)

        if scenario.cursor_count >= scenario.max_cursors:
            excess = scenario.cursor_count - scenario.max_cursors + 1
            await conn.execute(
                f"""
                DELETE FROM {_T_CURSOR}
                WHERE scenario_id = $1
                  AND token IN (
                      SELECT token
                      FROM {_T_CURSOR}
                      WHERE scenario_id = $1
                      ORDER BY last_used_seq ASC, token ASC
                      LIMIT $2
                  )
                """,
                scenario.scenario,
                excess,
            )
            await conn.execute(
                f"""
                UPDATE {_T_SCENARIO}
                SET cursor_count = cursor_count - $2
                WHERE scenario_id = $1
                """,
                scenario.scenario,
                excess,
            )
        row = await conn.fetchrow(
            f"""
            UPDATE {_T_SCENARIO}
            SET next_cursor_serial = next_cursor_serial + 1,
                next_use_serial = next_use_serial + 1,
                cursor_count = cursor_count + 1,
                cursors_issued = cursors_issued + 1
            WHERE scenario_id = $1
            RETURNING next_cursor_serial - 1 AS serial,
                      next_use_serial - 1 AS use_serial,
                      cursor_count
            """,
            scenario.scenario,
        )
        if row is None:
            raise ProviderContractError("scenario disappeared during cursor allocation")
        serial = _exact_int(row["serial"], name="cursor serial", minimum=1)
        use_serial = _exact_int(row["use_serial"], name="cursor use serial", minimum=1)
        cursor_count = _exact_int(row["cursor_count"], name="cursor_count", minimum=0)
        if serial > 0xFFFFFFFFFFFFFFFF:
            raise ProviderContractError("cursor serial exhausted")
        token = f"pc1_{scenario.cursor_nonce}_{serial:016x}"
        await conn.execute(
            f"""
            INSERT INTO {_T_CURSOR}
                (scenario_id, token, snapshot_id, last_record_id, last_revision,
                 last_used_seq, issued_seq)
            VALUES ($1, $2, $3, $4, $5::numeric, $6, $6)
            """,
            scenario.scenario,
            token,
            snapshot_id,
            last_record_id,
            last_revision,
            use_serial,
        )
        if cursor_count > scenario.max_cursors:
            raise ProviderContractError("cursor retention exceeded its declared bound")
        return ProviderCursor(token)

    async def _bump_fault(
        self,
        conn: asyncpg.Connection,
        scenario: str,
        *,
        withheld_pages: int,
        withheld_bytes: int,
    ) -> None:
        await conn.execute(
            f"""
            UPDATE {_T_SCENARIO}
            SET fault_gates_tripped = fault_gates_tripped + 1,
                withheld_pages = withheld_pages + $2,
                withheld_payload_bytes = withheld_payload_bytes + $3
            WHERE scenario_id = $1
            """,
            scenario,
            withheld_pages,
            withheld_bytes,
        )

    async def _bump_success(
        self,
        conn: asyncpg.Connection,
        scenario: str,
        records_returned: int,
        payload_bytes: int,
    ) -> None:
        await conn.execute(
            f"""
            UPDATE {_T_SCENARIO}
            SET pages_returned = pages_returned + 1,
                records_returned = records_returned + $2,
                payload_bytes = payload_bytes + $3
            WHERE scenario_id = $1
            """,
            scenario,
            records_returned,
            payload_bytes,
        )


__all__ = [
    "PgPagedProvider",
    "PgProviderRecordIdentity",
    "PgProviderScenarioState",
]

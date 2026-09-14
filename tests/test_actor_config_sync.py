"""Tests for ``sync_actor_config``: drift detection, forced overwrite, and UPSERT.

Unit-tier tests use a fake ``asyncpg.Connection``; integration-tier tests
use a real Postgres container via ``pg_conn``.
"""

from dataclasses import dataclass, field
from typing import Any

import asyncpg
import pytest

from taskq._ids import new_base62
from taskq._json import dumps_str, loads
from taskq.actor_config import ActorConfig
from taskq.exceptions import ActorConfigDriftList
from taskq.worker.startup import sync_actor_config


@dataclass
class FakeRecord:
    """A record-like object that supports dict-style key access."""

    _fields: dict[str, object] = field(default_factory=dict[str, object])

    def __getitem__(self, key: str) -> object:
        return self._fields[key]


class FakeAsyncpgConnection:
    """A test-double for ``asyncpg.Connection`` that records SELECT/UPSERT calls."""

    def __init__(self) -> None:
        self._select_rows: list[FakeRecord] = []
        self._transaction_count: int = 0
        self._fetch_calls: list[tuple[str, list[Any]]] = []
        self._execute_calls: list[tuple[str, list[Any]]] = []

    def set_select_rows(self, rows: list[FakeRecord]) -> None:
        self._select_rows = list(rows)

    @property
    def transaction_count(self) -> int:
        return self._transaction_count

    async def fetch(self, query: str, *params: Any) -> list[FakeRecord]:
        self._fetch_calls.append((query, list(params)))
        return list(self._select_rows)

    async def execute(self, query: str, *params: Any) -> str:
        self._execute_calls.append((query, list(params)))
        return "OK"

    def transaction(self) -> "FakeTransaction":
        return FakeTransaction(self)


class FakeTransaction:
    """Async context manager that records enter/exit and can fail."""

    def __init__(self, fake_conn: FakeAsyncpgConnection) -> None:
        self._conn = fake_conn
        self._entered = False

    async def __aenter__(self) -> "FakeTransaction":
        self._conn._transaction_count += 1
        self._entered = True
        return self

    async def __aexit__(self, *args: object) -> None:
        if not self._entered:
            raise RuntimeError("transaction exited without entering")


def _make_record(
    actor: str,
    max_concurrent: int | None = None,
    max_pending: int | None = None,
    queue: str = "default",
    result_ttl: float | None = None,
    metadata: dict[str, object] | None = None,
) -> FakeRecord:
    md = metadata if metadata is not None else {}
    return FakeRecord(
        {
            "actor": actor,
            "max_concurrent": max_concurrent,
            "max_pending": max_pending,
            "queue": queue,
            "result_ttl": result_ttl,
            "metadata": dumps_str(md),
        }
    )


def _make_config(
    actor: str,
    max_concurrent: int | None = None,
    max_pending: int | None = None,
    queue: str = "default",
    result_ttl: float | None = None,
    metadata: dict[str, object] | None = None,
) -> ActorConfig:
    return ActorConfig(
        actor=actor,
        max_concurrent=max_concurrent,
        max_pending=max_pending,
        queue=queue,
        result_ttl=result_ttl,
        metadata=metadata if metadata is not None else {},
    )


# ── Helpers for integration tests ────────────────────────────────────────────


async def _ensure_schema(conn: asyncpg.Connection, schema: str) -> None:
    await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    await conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
    await conn.execute(f"""
        CREATE TABLE IF NOT EXISTS "{schema}".actor_config (
            actor          text PRIMARY KEY,
            max_concurrent int,
            max_pending    int,
            queue          text NOT NULL,
            result_ttl     float,
            metadata       jsonb NOT NULL DEFAULT '{{}}'::jsonb,
            updated_at     timestamptz NOT NULL DEFAULT now()
        )
    """)


async def _select_configs(conn: asyncpg.Connection, schema: str) -> list[dict[str, object]]:
    rows = await conn.fetch(
        f'SELECT actor, max_concurrent, max_pending, queue, result_ttl, metadata FROM "{schema}".actor_config ORDER BY actor'
    )
    return [dict(row) for row in rows]


# ═══════════════════════════════════════════════════════════════════════════════
# Unit-tier tests (fake connection)
# ═══════════════════════════════════════════════════════════════════════════════


# ── Drift detection with force=False ──────────────────────────────────


@pytest.mark.asyncio
async def test_capacity_divergence_max_concurrent_does_not_raise() -> None:
    """max_concurrent divergence is an expected operator override, not an error.

    Pre-populate SELECT with max_concurrent=5 for actor "X"; register
    max_concurrent=3. sync_actor_config must NOT raise — the stored
    value is authoritative and capacity divergence is never fatal,
    regardless of ``force``.
    """
    fake_conn = FakeAsyncpgConnection()
    fake_conn.set_select_rows([_make_record("X", max_concurrent=5, queue="default")])

    await sync_actor_config(
        fake_conn,  # pyright: ignore[reportArgumentType] Why: FakeAsyncpgConnection is a unit-test double; real asyncpg.Connection subtyping would require protocol-level mocking
        [_make_config("X", max_concurrent=3)],
        force=False,
    )

    # UPSERT still executed; capacity divergence never blocks startup.
    assert len(fake_conn._execute_calls) == 1


# ── Multi-field drift ────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_multi_field_drift_only_metadata_raises() -> None:
    """Mixed drift: max_concurrent (capacity) + queue (assignment) + metadata
    (structural) differ.

    Only metadata raises; the queue assignment is operator-owned once a row
    exists (moved by `taskq actor-config move-queue`) and a differing
    literal is the rolling-deploy window of a move, not a bug — while
    max_concurrent divergence is silently accepted as an operator override.
    """
    fake_conn = FakeAsyncpgConnection()
    fake_conn.set_select_rows(
        [
            _make_record(
                "X",
                max_concurrent=5,
                queue="default",
                metadata={"x": 1},
            )
        ]
    )

    with pytest.raises(ActorConfigDriftList) as exc_info:
        await sync_actor_config(
            fake_conn,  # pyright: ignore[reportArgumentType] Why: FakeAsyncpgConnection is a unit-test double; real asyncpg.Connection subtyping would require protocol-level mocking
            [
                _make_config(
                    "X",
                    max_concurrent=3,
                    queue="critical",
                    metadata={"x": 2},
                )
            ],
            force=False,
        )

    drift_list = exc_info.value
    assert len(drift_list.drifts) == 1

    fields = {d.field for d in drift_list.drifts}
    assert fields == {"metadata"}


# ── force=True path ──────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_force_true_drift_proceeds_with_upsert() -> None:
    """force=True path: drift detected, UPSERT proceeds, returns None."""
    fake_conn = FakeAsyncpgConnection()
    fake_conn.set_select_rows([_make_record("X", max_concurrent=5, queue="default")])

    result = await sync_actor_config(
        fake_conn,  # pyright: ignore[reportArgumentType] Why: FakeAsyncpgConnection is a unit-test double; real asyncpg.Connection subtyping would require protocol-level mocking
        [_make_config("X", max_concurrent=3)],
        force=True,
    )

    assert result is None
    # UPSERT executed despite drift
    assert len(fake_conn._execute_calls) == 1


# ── Empty actor_configs list ─────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_empty_actor_configs_noop() -> None:
    """Empty actor_configs list: no SELECT, no UPSERT, returns None."""
    fake_conn = FakeAsyncpgConnection()

    result = await sync_actor_config(
        fake_conn,  # pyright: ignore[reportArgumentType] Why: FakeAsyncpgConnection is a unit-test double; real asyncpg.Connection subtyping would require protocol-level mocking
        [],
    )

    assert result is None
    assert len(fake_conn._fetch_calls) == 0
    assert len(fake_conn._execute_calls) == 0


# ── Metadata structural equality ─────────────────────────────────────────────


@pytest.mark.asyncio
async def test_metadata_structural_equality_no_drift() -> None:
    """Metadata structural equality: stored {"a": 1, "b": 2} vs
    registered {"b": 2, "a": 1} — no drift raised, UPSERT proceeds.
    """
    fake_conn = FakeAsyncpgConnection()
    fake_conn.set_select_rows(
        [
            _make_record(
                "X",
                max_concurrent=3,
                queue="default",
                metadata={"a": 1, "b": 2},
            )
        ]
    )

    await sync_actor_config(
        fake_conn,  # pyright: ignore[reportArgumentType] Why: FakeAsyncpgConnection is a unit-test double; real asyncpg.Connection subtyping would require protocol-level mocking
        [_make_config("X", max_concurrent=3, metadata={"b": 2, "a": 1})],
        force=False,
    )

    # No ActorConfigDriftList raised; UPSERT executed
    assert len(fake_conn._execute_calls) == 1


# ── Single transaction ───────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_single_transaction_wraps_both_phases() -> None:
    """Single transaction: assert conn.transaction() is entered exactly once
    across the SELECT and UPSERT.
    """
    fake_conn = FakeAsyncpgConnection()
    fake_conn.set_select_rows([])

    await sync_actor_config(
        fake_conn,  # pyright: ignore[reportArgumentType] Why: FakeAsyncpgConnection is a unit-test double; real asyncpg.Connection subtyping would require protocol-level mocking
        [_make_config("X", max_concurrent=3)],
    )

    assert fake_conn.transaction_count == 1


# ── Invalid schema ───────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_invalid_schema_raises_value_error() -> None:
    """Invalid schema identifier raises ValueError before any I/O."""
    fake_conn = FakeAsyncpgConnection()

    with pytest.raises(ValueError, match="invalid schema"):
        await sync_actor_config(
            fake_conn,  # pyright: ignore[reportArgumentType] Why: FakeAsyncpgConnection is a unit-test double; real asyncpg.Connection subtyping would require protocol-level mocking
            [_make_config("X")],
            schema="bad; DROP TABLE",
        )

    assert fake_conn._fetch_calls == []
    assert fake_conn._execute_calls == []


# ── New actor (no stored row) — no drift ─────────────────────────────────────


@pytest.mark.asyncio
async def test_new_actor_no_stored_row_no_drift() -> None:
    """A new actor with no stored row produces no drift error and UPSERT proceeds."""
    fake_conn = FakeAsyncpgConnection()
    fake_conn.set_select_rows([])

    await sync_actor_config(
        fake_conn,  # pyright: ignore[reportArgumentType] Why: FakeAsyncpgConnection is a unit-test double; real asyncpg.Connection subtyping would require protocol-level mocking
        [_make_config("X", max_concurrent=3)],
        force=False,
    )

    # No ActorConfigDriftList raised; UPSERT executed
    assert len(fake_conn._execute_calls) == 1


@pytest.mark.asyncio
async def test_metadata_nul_byte_rejected_before_upsert() -> None:
    """``@actor(metadata={...})`` is user-supplied. A NUL (U+0000) anywhere
    in it must be rejected by the jsonb NUL guard *before* the UPSERT is
    executed — a NUL reaching Postgres' jsonb parser raises
    ``UntranslatableCharacterError`` (a ``PostgresError``), which is exactly
    the exception class treated as retryable infra failure elsewhere in the
    worker; the guard must fire first so this surfaces as an immediate,
    unambiguous ``ValueError`` instead."""
    fake_conn = FakeAsyncpgConnection()
    fake_conn.set_select_rows([])

    with pytest.raises(ValueError, match="NUL"):
        await sync_actor_config(
            fake_conn,  # pyright: ignore[reportArgumentType] Why: FakeAsyncpgConnection is a unit-test double; real asyncpg.Connection subtyping would require protocol-level mocking
            [_make_config("X", metadata={"note": "bad\x00value"})],
            force=False,
        )

    assert fake_conn._execute_calls == []


# ── max_pending upsert array ────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_max_pending_int_in_upsert_array() -> None:
    """sync_actor_config with max_pending=100 passes 100 in the upsert mp_array."""
    fake_conn = FakeAsyncpgConnection()
    fake_conn.set_select_rows([])

    await sync_actor_config(
        fake_conn,  # pyright: ignore[reportArgumentType] Why: FakeAsyncpgConnection is a unit-test double; real asyncpg.Connection subtyping would require protocol-level mocking
        [_make_config("X", max_concurrent=None, max_pending=100)],
        force=True,
    )

    assert len(fake_conn._execute_calls) == 1
    _sql, params = fake_conn._execute_calls[0]
    # params order: actor_names, mc_array, mp_array, queue_array, result_ttl_array, metadata_array
    assert params[2] == [100]


@pytest.mark.asyncio
async def test_max_pending_none_in_upsert_array() -> None:
    """sync_actor_config with max_pending=None passes None in the upsert mp_array."""
    fake_conn = FakeAsyncpgConnection()
    fake_conn.set_select_rows([])

    await sync_actor_config(
        fake_conn,  # pyright: ignore[reportArgumentType] Why: FakeAsyncpgConnection is a unit-test double; real asyncpg.Connection subtyping would require protocol-level mocking
        [_make_config("X", max_concurrent=None, max_pending=None)],
        force=True,
    )

    assert len(fake_conn._execute_calls) == 1
    _sql, params = fake_conn._execute_calls[0]
    assert params[2] == [None]


# ── max_pending drift ───────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_capacity_divergence_max_pending_does_not_raise() -> None:
    """max_pending divergence with force=False does not raise.

    Pre-populate SELECT with max_pending=50 for actor "X"; register
    max_pending=100. sync_actor_config must not raise, and the UPSERT
    still runs (the stored 50 is preserved by the UPSERT's SQL, not by
    control flow — see test_upsert_sql_preserves_capacity_on_conflict).
    """
    fake_conn = FakeAsyncpgConnection()
    fake_conn.set_select_rows(
        [_make_record("X", max_concurrent=None, max_pending=50, queue="default")]
    )

    await sync_actor_config(
        fake_conn,  # pyright: ignore[reportArgumentType] Why: FakeAsyncpgConnection is a unit-test double; real asyncpg.Connection subtyping would require protocol-level mocking
        [_make_config("X", max_concurrent=None, max_pending=100)],
        force=False,
    )

    assert len(fake_conn._execute_calls) == 1


@pytest.mark.asyncio
async def test_capacity_divergence_result_ttl_does_not_raise() -> None:
    """result_ttl divergence is an expected operator override, not an error.

    Pre-populate SELECT with result_ttl=60.0 for actor "X"; register
    result_ttl=120.0. sync_actor_config must NOT raise — the stored
    value is authoritative and capacity divergence is never fatal,
    regardless of ``force``.
    """
    fake_conn = FakeAsyncpgConnection()
    fake_conn.set_select_rows([_make_record("X", result_ttl=60.0, queue="default")])

    await sync_actor_config(
        fake_conn,  # pyright: ignore[reportArgumentType] Why: FakeAsyncpgConnection is a unit-test double; real asyncpg.Connection subtyping would require protocol-level mocking
        [_make_config("X", result_ttl=120.0)],
        force=False,
    )

    assert len(fake_conn._execute_calls) == 1


# ── UPSERT SQL shape ─────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_upsert_sql_preserves_operator_owned_columns_on_conflict() -> None:
    """The rendered UPSERT's ON CONFLICT clause never assigns the capacity
    columns or the queue assignment.

    This is what actually preserves a stored capacity value — and, across
    the rolling-deploy window of `taskq actor-config move-queue`, the moved
    queue assignment — across startups: a worker still carrying the old
    literal boots, and its UPSERT cannot flip the row back.
    """
    fake_conn = FakeAsyncpgConnection()
    fake_conn.set_select_rows([])

    await sync_actor_config(
        fake_conn,  # pyright: ignore[reportArgumentType] Why: FakeAsyncpgConnection is a unit-test double; real asyncpg.Connection subtyping would require protocol-level mocking
        [_make_config("X", max_concurrent=3, max_pending=100, result_ttl=60.0)],
    )

    sql, _params = fake_conn._execute_calls[0]
    on_conflict = sql.split("DO UPDATE SET", 1)[1]
    assert "max_concurrent" not in on_conflict
    assert "max_pending" not in on_conflict
    assert "result_ttl" not in on_conflict
    assert "queue" not in on_conflict
    assert "metadata" in on_conflict


# ═══════════════════════════════════════════════════════════════════════════════
# Integration-tier tests (real PG via pg_conn fixture)
# ═══════════════════════════════════════════════════════════════════════════════


# ── sync three actors on empty table ──────────────────────────────────


@pytest.mark.asyncio
@pytest.mark.integration
async def test_integration_sync_three_actors_empty_table(
    pg_conn: asyncpg.Connection,
) -> None:
    """start with empty actor_config; sync three actors;
    assert all three rows present with expected values.
    """
    schema = f"tacs_{new_base62()}".lower()
    await _ensure_schema(pg_conn, schema)

    configs = [
        _make_config("a", max_concurrent=5, queue="default"),
        _make_config("b", max_concurrent=10, queue="critical"),
        _make_config("c", max_concurrent=None, queue="low"),
    ]

    await sync_actor_config(pg_conn, configs, schema=schema)

    rows = await _select_configs(pg_conn, schema)
    assert len(rows) == 3

    by_actor = {row["actor"]: row for row in rows}
    assert by_actor["a"]["max_concurrent"] == 5
    assert by_actor["a"]["queue"] == "default"
    assert by_actor["b"]["max_concurrent"] == 10
    assert by_actor["b"]["queue"] == "critical"
    assert by_actor["c"]["max_concurrent"] is None
    assert by_actor["c"]["queue"] == "low"


# ── Re-sync with no changes ─────────────────────────────────────────────────


@pytest.mark.asyncio
@pytest.mark.integration
async def test_integration_resync_no_changes_no_error(
    pg_conn: asyncpg.Connection,
) -> None:
    """Re-sync with no changes: no error, row count unchanged, data unchanged."""
    schema = f"tacs_{new_base62()}".lower()
    await _ensure_schema(pg_conn, schema)

    configs = [_make_config("a", max_concurrent=5, queue="default")]
    await sync_actor_config(pg_conn, configs, schema=schema)

    # Re-sync same configs — no drift exception
    await sync_actor_config(pg_conn, configs, schema=schema)

    rows = await _select_configs(pg_conn, schema)
    assert len(rows) == 1
    assert rows[0]["max_concurrent"] == 5
    assert rows[0]["queue"] == "default"


# ── Re-sync with drift and force=False ───────────────────────────────────────


@pytest.mark.asyncio
@pytest.mark.integration
async def test_integration_queue_drift_force_false_boots_and_row_unchanged(
    pg_conn: asyncpg.Connection,
) -> None:
    """Re-sync with a differing queue (assignment drift) and force=False:
    no exception — this is the rolling-deploy window of a move — and the
    stored assignment survives the boot (the UPSERT never rewrites it).
    """
    schema = f"tacs_{new_base62()}".lower()
    await _ensure_schema(pg_conn, schema)

    original = _make_config("a", max_concurrent=5, queue="default")
    await sync_actor_config(pg_conn, [original], schema=schema)

    changed = [_make_config("a", max_concurrent=5, queue="critical")]

    await sync_actor_config(pg_conn, changed, force=False, schema=schema)

    rows = await _select_configs(pg_conn, schema)
    assert len(rows) == 1
    assert rows[0]["max_concurrent"] == 5
    assert rows[0]["queue"] == "default", "a differing literal must not flip the stored assignment"


@pytest.mark.asyncio
@pytest.mark.integration
async def test_integration_metadata_drift_force_false_raises_table_unchanged(
    pg_conn: asyncpg.Connection,
) -> None:
    """Re-sync with differing metadata (structural) and force=False:
    ActorConfigDriftList raised; table is unchanged.
    """
    schema = f"tacs_{new_base62()}".lower()
    await _ensure_schema(pg_conn, schema)

    original = _make_config("a", max_concurrent=5, queue="default", metadata={"x": 1})
    await sync_actor_config(pg_conn, [original], schema=schema)

    changed = [_make_config("a", max_concurrent=5, queue="default", metadata={"x": 2})]

    with pytest.raises(ActorConfigDriftList) as exc_info:
        await sync_actor_config(pg_conn, changed, force=False, schema=schema)

    assert {d.field for d in exc_info.value.drifts} == {"metadata"}

    rows = await _select_configs(pg_conn, schema)
    assert len(rows) == 1
    assert rows[0]["max_concurrent"] == 5
    assert rows[0]["queue"] == "default"


@pytest.mark.asyncio
@pytest.mark.integration
async def test_integration_capacity_drift_force_false_does_not_raise(
    pg_conn: asyncpg.Connection,
) -> None:
    """Re-sync with a differing max_concurrent (capacity) and force=False:
    no exception, and the stored value is unchanged (operator-owned).
    """
    schema = f"tacs_{new_base62()}".lower()
    await _ensure_schema(pg_conn, schema)

    original = _make_config("a", max_concurrent=5, queue="default")
    await sync_actor_config(pg_conn, [original], schema=schema)

    changed = [_make_config("a", max_concurrent=3, queue="default")]
    await sync_actor_config(pg_conn, changed, force=False, schema=schema)

    rows = await _select_configs(pg_conn, schema)
    assert len(rows) == 1
    assert rows[0]["max_concurrent"] == 5, "stored capacity must survive a differing literal"
    assert rows[0]["queue"] == "default"


# ── Re-sync with force=True ──────────────────────────────────────────────────


@pytest.mark.asyncio
@pytest.mark.integration
async def test_integration_queue_drift_force_true_preserves_stored_queue(
    pg_conn: asyncpg.Connection,
) -> None:
    """Re-sync with a differing queue and force=True: the stored queue is
    preserved — force governs metadata only, and a force-boot rewriting the
    assignment from a stale literal is exactly the move-undo hazard the
    conflict clause forecloses.
    """
    schema = f"tacs_{new_base62()}".lower()
    await _ensure_schema(pg_conn, schema)

    original = _make_config("a", max_concurrent=5, queue="default")
    await sync_actor_config(pg_conn, [original], schema=schema)

    changed = [_make_config("a", max_concurrent=5, queue="critical")]

    await sync_actor_config(pg_conn, changed, force=True, schema=schema)

    rows = await _select_configs(pg_conn, schema)
    assert len(rows) == 1
    assert rows[0]["queue"] == "default"


@pytest.mark.asyncio
@pytest.mark.integration
async def test_integration_metadata_drift_force_true_overwrites(
    pg_conn: asyncpg.Connection,
) -> None:
    """Re-sync with differing metadata and force=True: the stored metadata
    is updated (the flag's remaining purpose).
    """
    schema = f"tacs_{new_base62()}".lower()
    await _ensure_schema(pg_conn, schema)

    original = _make_config("a", max_concurrent=5, queue="default", metadata={"x": 1})
    await sync_actor_config(pg_conn, [original], schema=schema)

    changed = [_make_config("a", max_concurrent=5, queue="default", metadata={"x": 2})]

    await sync_actor_config(pg_conn, changed, force=True, schema=schema)

    rows = await _select_configs(pg_conn, schema)
    assert len(rows) == 1
    assert rows[0]["queue"] == "default"
    assert loads(rows[0]["metadata"]) == {"x": 2}


@pytest.mark.asyncio
@pytest.mark.integration
async def test_integration_force_true_never_overwrites_capacity_or_queue(
    pg_conn: asyncpg.Connection,
) -> None:
    """force=True overwrites metadata drift but never touches the capacity
    fields or the queue assignment.

    Registers a differing max_concurrent AND queue simultaneously with
    force=True: neither is overwritten — both are operator-owned once the
    row exists.
    """
    schema = f"tacs_{new_base62()}".lower()
    await _ensure_schema(pg_conn, schema)

    original = _make_config("a", max_concurrent=5, queue="default")
    await sync_actor_config(pg_conn, [original], schema=schema)

    changed = [_make_config("a", max_concurrent=3, queue="critical")]
    await sync_actor_config(pg_conn, changed, force=True, schema=schema)

    rows = await _select_configs(pg_conn, schema)
    assert len(rows) == 1
    assert rows[0]["queue"] == "default", "force=True must not overwrite the queue assignment"
    assert rows[0]["max_concurrent"] == 5, "force=True must not overwrite capacity fields"


# ── max_pending persistence ───────────────────────────────────────────────


@pytest.mark.asyncio
@pytest.mark.integration
async def test_integration_max_pending_persisted(
    pg_conn: asyncpg.Connection,
) -> None:
    """After sync_actor_config, query returns the persisted max_pending."""
    schema = f"tacs_{new_base62()}".lower()
    await _ensure_schema(pg_conn, schema)

    configs = [_make_config("a", max_concurrent=None, max_pending=100)]
    await sync_actor_config(pg_conn, configs, schema=schema)

    rows = await _select_configs(pg_conn, schema)
    assert len(rows) == 1
    assert rows[0]["max_pending"] == 100


@pytest.mark.asyncio
@pytest.mark.integration
async def test_integration_max_pending_divergence_survives_resync(
    pg_conn: asyncpg.Connection,
) -> None:
    """Pre-seed actor_config with max_pending=50, register with max_pending=100,
    sync_actor_config: no exception, and the stored 50 is preserved (operator-owned).
    """
    schema = f"tacs_{new_base62()}".lower()
    await _ensure_schema(pg_conn, schema)

    original = _make_config("a", max_concurrent=None, max_pending=50)
    await sync_actor_config(pg_conn, [original], schema=schema)

    changed = [_make_config("a", max_concurrent=None, max_pending=100)]
    await sync_actor_config(pg_conn, changed, force=False, schema=schema)

    rows = await _select_configs(pg_conn, schema)
    assert len(rows) == 1
    assert rows[0]["max_pending"] == 50


@pytest.mark.asyncio
@pytest.mark.integration
async def test_integration_result_ttl_divergence_survives_resync(
    pg_conn: asyncpg.Connection,
) -> None:
    """Pre-seed actor_config with result_ttl=60.0, register with result_ttl=120.0,
    sync_actor_config: no exception, and the stored 60.0 is preserved (operator-owned).
    """
    schema = f"tacs_{new_base62()}".lower()
    await _ensure_schema(pg_conn, schema)

    original = _make_config("a", max_concurrent=5, queue="default", result_ttl=60.0)
    await sync_actor_config(pg_conn, [original], schema=schema)

    changed = [_make_config("a", max_concurrent=5, queue="default", result_ttl=120.0)]
    await sync_actor_config(pg_conn, changed, force=False, schema=schema)

    rows = await _select_configs(pg_conn, schema)
    assert len(rows) == 1
    assert rows[0]["result_ttl"] == 60.0, "stored capacity must survive a differing literal"
    assert rows[0]["queue"] == "default"


@pytest.mark.asyncio
@pytest.mark.integration
async def test_integration_max_pending_none_round_trip(
    pg_conn: asyncpg.Connection,
) -> None:
    """Register an actor with no max_pending, sync, query — column is SQL NULL."""
    schema = f"tacs_{new_base62()}".lower()
    await _ensure_schema(pg_conn, schema)

    configs = [_make_config("a", max_concurrent=None, max_pending=None)]
    await sync_actor_config(pg_conn, configs, schema=schema)

    rows = await _select_configs(pg_conn, schema)
    assert len(rows) == 1
    assert rows[0]["max_pending"] is None

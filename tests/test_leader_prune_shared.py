"""Unit pins for ``worker/_leader_shared``'s prune-family batch machinery.

The prune family (archive-and-delete terminal jobs) runs every batch
through the same bounded-batch machinery the backend sweeps use, plus a
drain gate and per-status retention loops. Three behaviors were
unpinned:

* the schema-guard contract: the SQL renderers interpolate the schema
  identifier, so a non-identifier schema must raise before any SQL is
  built (WorkerSettings validates schema_name at load; the guard is the
  defense's own contract should a caller ever bypass that load);
* a deadline-family abort inside a batch counts against the
  ``SweepBatchSizer`` (the breaker that latches to smaller batches) and
  re-raises — a stopped drain is a pause, never a silent rollback;
* the drain gate ends the drain between batches, and a full batch
  (``batch_total == size``) loops for more, the loop's exit conditions.

The fake connection answers the exact duck-typing surface the machinery
uses (``fetch`` / ``execute`` / ``transaction``), scripted per statement.
"""

from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._sweeps import SweepBatchSizer
from taskq.worker._leader_shared import (
    _run_prune_archive_batch,  # pyright: ignore[reportPrivateUsage]  # Why: the unit under test is the batch machinery; its callers are pinned end-to-end in tests/test_leader.py
    _run_prune_batch,  # pyright: ignore[reportPrivateUsage]
    cleanup_stale_workers,  # pyright: ignore[reportPrivateUsage]
    complete_stale_batches_sql,  # pyright: ignore[reportPrivateUsage]
    prune_terminal_jobs,
)

_CLOCK_SQL = "statement_timestamp"


class _ScriptedConn:
    """asyncpg.Connection stand-in that routes by statement shape.

    The prune loop walks ``TERMINAL_STATUSES`` (a set: its iteration order
    is process-dependent), so a position-scripted response list would be
    a flake. Candidates are keyed by the bound status instead; unlisted
    statuses answer an empty window (the honest drained answer).
    """

    def __init__(
        self,
        *,
        candidate_windows: dict[str, list[list[dict[str, Any]]]] | None = None,
        write_rows: dict[str, list[dict[str, Any]]] | None = None,
        fail_other_fetches_with: BaseException | None = None,
    ) -> None:
        # Consumable per status: a full batch loops for another window,
        # and the follow-up window must see the drained world, not the
        # same rows again (a stateless fake here loops forever).
        self._windows: dict[str, list[list[dict[str, Any]]]] = {
            k: list(v) for k, v in (candidate_windows or {}).items()
        }
        self._writes = write_rows or {}
        self._fail_other = fail_other_fetches_with
        self.fetch_calls: list[tuple[str, tuple[object, ...]]] = []
        self.execute_calls: list[str] = []

    async def fetch(self, sql: str, *args: object) -> Sequence[dict[str, Any]]:
        self.fetch_calls.append((sql, args))
        if "current_setting(" in sql:
            return [{"current_setting": "5s"}]
        if self._fail_other is not None and "ORDER BY finished_at" not in sql:
            raise self._fail_other
        status = str(args[0])
        if "ORDER BY finished_at" in sql:  # the candidate window
            windows = self._windows.get(status, [])
            return windows.pop(0) if windows else []
        return list(self._writes.get(status, []))  # the archive write

    async def execute(self, sql: str, *args: object) -> str:
        self.execute_calls.append(sql)
        return "UPDATE 1"

    async def fetchval(self, sql: str, *args: object) -> Any:
        # The prune's expiry reference instant: the DB-clock read the
        # sweep issues before its loops.
        return datetime.now(UTC)

    def transaction(self) -> "_ScriptedTransaction":
        return _ScriptedTransaction()


class _ScriptedTransaction:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, *args: object) -> None:
        return None


class _RecordingSizer(SweepBatchSizer):
    """Sizer that records which callbacks the batch machinery made."""

    def __init__(self) -> None:
        super().__init__(100, 4, 3, 600.0)
        self.timeouts = 0
        self.successes = 0

    def on_timeout(self) -> None:
        self.timeouts += 1
        super().on_timeout()

    def on_success(self) -> None:
        self.successes += 1
        super().on_success()


# ── The schema guards ────────────────────────────────────────────────────


async def test_cleanup_stale_workers_refuses_a_non_identifier_schema() -> None:
    """The statement interpolates the schema identifier, so a non-identifier
    schema must raise before any SQL is built — the same fail-loud contract
    the other SQL renderers carry."""
    conn = _ScriptedConn()

    with pytest.raises(ValueError, match="invalid schema identifier"):
        await cleanup_stale_workers(
            conn,  # type: ignore[arg-type]
            worker_id=new_uuid(),
            staleness=timedelta(days=1),
            schema='bad"; DROP TABLE workers',
        )

    assert conn.execute_calls == [], (
        "the guard must fire before the statement, not after a malformed render reaches the driver"
    )


def test_complete_stale_batches_sql_refuses_a_non_identifier_schema() -> None:
    with pytest.raises(ValueError, match="invalid schema identifier"):
        complete_stale_batches_sql('bad"; DROP TABLE batches')


# ── The deadline breaker wiring ──────────────────────────────────────────


async def test_a_cancelled_batch_counts_against_the_sizer_and_raises() -> None:
    """The server cancelling the batch statement (statement_timeout) is the
    breaker's control signal: the batch machinery must feed it to the
    sizer AND re-raise, so the caller's failure path retries at the
    latched reduced tier while everything committed before stays."""
    sizer = _RecordingSizer()
    conn = _ScriptedConn(fail_other_fetches_with=asyncpg.QueryCanceledError("statement_timeout"))

    with pytest.raises(asyncpg.QueryCanceledError):
        await _run_prune_batch(
            conn,  # type: ignore[arg-type]  # Why: the duck-typed ConnLike the machinery consumes
            "DELETE FROM jobs WHERE id = ANY($1)",
            [1],
            statement_timeout_ms=500,
            sizer=sizer,
        )

    assert sizer.timeouts == 1, (
        "a cancelled batch must count against the breaker: without the "
        "signal the next batch re-runs at full size and cancels again, a "
        "drain that can never make progress"
    )
    assert sizer.successes == 0


async def test_a_cancelled_archive_write_counts_against_the_sizer_and_raises() -> None:
    """Same contract on the archive variant: the write statement's deadline
    abort is the breaker's signal too (the candidate window and the
    archive write are two statements of ONE batch)."""

    class _WriteCancels(_ScriptedConn):
        async def fetch(self, sql: str, *args: object) -> Sequence[dict[str, Any]]:
            if "current_setting(" not in sql and "ORDER BY finished_at" not in sql:
                raise asyncpg.QueryCanceledError("statement_timeout")  # the archive write
            return await super().fetch(sql, *args)

    sizer = _RecordingSizer()
    conn = _WriteCancels(candidate_windows={"succeeded": [[{"id": 1}]]})

    with pytest.raises(asyncpg.QueryCanceledError):
        await _run_prune_archive_batch(
            conn,  # type: ignore[arg-type]
            candidate_sql="SELECT id FROM jobs WHERE status = $1",
            write_sql="DELETE FROM jobs WHERE id = ANY($3)",
            status="succeeded",
            retention=timedelta(days=1),
            size=100,
            archive_interval=timedelta(days=30),
            actor=None,
            statement_timeout_ms=500,
            sizer=sizer,
        )

    assert sizer.timeouts == 1
    assert sizer.successes == 0


# ── The drain gate and the batch loop ───────────────────────────────────


async def test_a_closed_drain_gate_ends_the_drain_before_any_batch() -> None:
    """The drain gate is checked between batches: a closed gate must end
    the drain before the first batch statement runs (a paused drain is a
    no-op, not a final full batch)."""
    conn = _ScriptedConn()

    result = await prune_terminal_jobs(
        conn,  # type: ignore[arg-type]
        retention_per_status={"succeeded": timedelta(days=1)},
        archive_retention=timedelta(days=30),
        schema="taskq",
        drain_gate=lambda: False,
    )

    assert result.total_deleted == 0
    assert result.archived == 0
    batch_fetches = [call for call in conn.fetch_calls if "current_setting(" not in call[0]]
    assert batch_fetches == [], (
        "a closed drain gate must end the drain before the first batch "
        "statement: the gate exists to stop the drain between ticks"
    )


async def test_a_full_batch_loops_for_another() -> None:
    """``batch_total < size`` is the loop's exit condition: a batch that
    returned exactly ``size`` jobs may have more behind it, so the loop
    must run again (the drain gate re-checked) until a short batch or an
    empty one ends it."""
    size = 2
    conn = _ScriptedConn(
        candidate_windows={"failed": [[{"id": 1}], []]},
        write_rows={"failed": [{"actor": "email", "status": "failed", "cnt": size}]},
    )

    result = await prune_terminal_jobs(
        conn,  # type: ignore[arg-type]
        retention_per_status={"succeeded": timedelta(days=1)},
        archive_retention=timedelta(days=30),
        batch_size=size,
        schema="taskq",
    )

    failed_batches = [
        call
        for call in conn.fetch_calls
        if call[1][:1] == ("failed",) and "ORDER BY finished_at" in call[0]
    ]
    assert len(failed_batches) == 2, (
        f"a full batch must loop for another; ran {len(failed_batches)} batches for the full status"
    )
    assert result.total_deleted == size
    assert result.by_actor == {"email": size}
    assert result.by_status == {"failed": size}

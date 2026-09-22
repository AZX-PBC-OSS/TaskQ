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
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

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
            sweep_name="prune",
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
            sweep_name="prune",
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


# ── The breaker arms on ANY repeated batch failure ───────────────────────
#
# The reduced tier is the safety net for the next unknown prune failure
# mode, so the sizer must hear about every batch failure, not only the
# deadline family. The incident that motivated this pin (#358's class):
# an archive UniqueViolationError failed every prune batch while the
# sizer stayed unlatched (only QueryCanceledError/TimeoutError counted),
# so the drain re-ran the same full-size batch forever at the same tier
# with nothing arming and no metric naming the stall.

_UNEXPECTED_METRIC = "taskq.maintenance_leader.sweep_unexpected_errors"


@pytest.fixture
def unexpected_error_reader(monkeypatch: pytest.MonkeyPatch) -> InMemoryMetricReader:
    """Per-test OTel meter isolation for the unexpected-sweep-error counter.

    Same shape as ``sweep_metric_reader`` in tests/test_sweep_timeout_metrics.py:
    a fresh SDK instrument replaces the module singleton so the assertion
    reads only THIS test's emissions, and ``_otel_enabled`` is forced on
    because ``record_sweep_unexpected_error`` is a no-op while it is off.
    ``raising=False`` so the suite can run against a build where the
    instrument does not exist yet (the red state: nothing emits, the
    counter assertion fails on its own).
    """
    from opentelemetry.sdk.metrics import MeterProvider

    import taskq.obs as obs_mod
    import taskq.obs._otel as otel_mod

    reader = InMemoryMetricReader()
    meter = MeterProvider(metric_readers=[reader]).get_meter(
        obs_mod.INSTRUMENTATION_NAME,
        otel_mod._version(),  # pyright: ignore[reportPrivateUsage]  # Why: same private-helper access as tests/test_sweep_timeout_metrics.py's sweep_metric_reader fixture.
    )
    monkeypatch.setattr(
        otel_mod,
        "_sweep_unexpected_errors",
        meter.create_counter(_UNEXPECTED_METRIC, unit="1"),
        raising=False,
    )
    monkeypatch.setattr(otel_mod, "_otel_enabled", True)
    return reader


def _unexpected_error_count(reader: InMemoryMetricReader, sweep_name: str) -> int:
    from taskq.testing.otel import counter_data_points

    return sum(
        int(dp.value)
        for dp in counter_data_points(reader, _UNEXPECTED_METRIC)
        if dp.attributes == {"sweep_name": sweep_name}
    )


class _WriteViolatesNTimes(_ScriptedConn):
    """Scripted conn whose archive write raises the first N attempts.

    The injected failure is ``UniqueViolationError``, the non-deadline
    shape the archive-orphan incident produced: the candidate window
    answers, the lock-bearing write raises, the batch aborts. While the
    failures last every candidate window answers one eligible row (the
    backlog an incident leaves behind), so a failed attempt does not
    drain the world; after recovery the window answers one row, then the
    honest empty drain.
    """

    def __init__(
        self,
        *,
        fail_times: int,
        write_rows: dict[str, list[dict[str, Any]]] | None = None,
    ) -> None:
        super().__init__(write_rows=write_rows)
        self._fail_times = fail_times
        self._write_attempts = 0
        self._recovery_window_used = False
        self.candidate_sizes: list[int] = []

    async def fetch(self, sql: str, *args: object) -> Sequence[dict[str, Any]]:
        if "current_setting(" not in sql and "ORDER BY finished_at" not in sql:
            # The archive write: the batch's second statement.
            self._write_attempts += 1
            if self._write_attempts <= self._fail_times:
                raise asyncpg.exceptions.UniqueViolationError(
                    'duplicate key value violates unique constraint "jobs_archive_pkey"'
                )
            return await super().fetch(sql, *args)
        if "ORDER BY finished_at" in sql:
            # The candidate window: args are (status, retention, size[, actor]).
            self.candidate_sizes.append(int(args[2]))  # type: ignore[arg-type]
            if self._write_attempts < self._fail_times:
                return [{"id": 1}]  # a candidate waits behind every failing batch
            if str(args[0]) in self._writes and not self._recovery_window_used:
                # The recovering window: one row for a status whose write
                # is scripted to succeed, so the drain has work to land.
                self._recovery_window_used = True
                return [{"id": 1}]
            return []  # drained
        return await super().fetch(sql, *args)


async def test_a_non_deadline_batch_failure_counts_against_the_sizer_and_raises() -> None:
    """THE pin: a repeated non-deadline failure arms the reduced tier.

    The breaker's control signal is an aborted batch, and a batch that
    aborts on a UniqueViolation (or any other transient error the
    deadline family does not name) aborts exactly as hard as a cancelled
    one: the drain stops making progress either way, so the same
    failure-threshold contract must count it. Pre-fix the sizer never
    heard about these, the tier never armed, and the caller retried the
    same full-size batch forever.
    """
    sizer = _RecordingSizer()  # default_size 100, divisor 4, threshold 3
    conn = _ScriptedConn(
        fail_other_fetches_with=asyncpg.exceptions.UniqueViolationError("duplicate key")
    )

    for _ in range(3):
        with pytest.raises(asyncpg.exceptions.UniqueViolationError):
            await _run_prune_batch(
                conn,  # type: ignore[arg-type]  # Why: the duck-typed ConnLike the machinery consumes
                "DELETE FROM jobs WHERE id = ANY($1)",
                [1],
                statement_timeout_ms=500,
                sweep_name="prune",
                sizer=sizer,
            )

    assert sizer.timeouts == 3, (
        "every repeated batch failure must count against the breaker: with "
        "only deadline errors counted, an unknown failure mode re-runs the "
        "same full-size batch forever with the tier never arming"
    )
    assert sizer.successes == 0
    assert sizer.effective_size() == 25, (
        "after the failure threshold the reduced tier must be latched: the "
        "reduced tier is the safety net for the next unknown prune failure mode"
    )


async def test_a_non_deadline_archive_write_failure_counts_against_the_sizer() -> None:
    """Same contract on the archive variant, failing the write arm: the
    candidate window answers, the lock-bearing write raises, and that
    abort is the breaker's signal too (the two statements are ONE batch)."""

    sizer = _RecordingSizer()
    conn = _WriteViolatesNTimes(fail_times=3)

    for _ in range(3):
        with pytest.raises(asyncpg.exceptions.UniqueViolationError):
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
                sweep_name="prune",
                sizer=sizer,
            )

    assert sizer.timeouts == 3
    assert sizer.successes == 0
    assert sizer.effective_size() == 25, (
        "the archive variant must arm the same reduced tier on the same "
        "repeated non-deadline failure"
    )


async def test_a_deadline_failure_stays_off_the_unexpected_error_counter(
    unexpected_error_reader: InMemoryMetricReader,
) -> None:
    """The classification control: the deadline family belongs on
    ``sweep_timeouts`` (the loops record it), not on the unexpected-error
    counter - one failure, one counter, no double counting."""
    sizer = _RecordingSizer()

    class _WriteCancels(_ScriptedConn):
        async def fetch(self, sql: str, *args: object) -> Sequence[dict[str, Any]]:
            if "current_setting(" not in sql and "ORDER BY finished_at" not in sql:
                raise asyncpg.QueryCanceledError("statement_timeout")  # the archive write
            return await super().fetch(sql, *args)

    conn = _WriteCancels(candidate_windows={"succeeded": [[{"id": 1}]]})

    with pytest.raises(asyncpg.QueryCanceledError):
        await prune_terminal_jobs(
            conn,  # type: ignore[arg-type]
            retention_per_status={"succeeded": timedelta(days=1)},
            archive_retention=timedelta(days=30),
            schema="taskq",
            sizer=sizer,
        )

    assert sizer.timeouts == 1
    assert _unexpected_error_count(unexpected_error_reader, "prune") == 0, (
        "a deadline-family abort is already counted on sweep_timeouts; "
        "counting it again as unexpected would double-count one failure"
    )


async def test_an_unexpected_prune_batch_error_is_counted_on_the_metric_plane(
    unexpected_error_reader: InMemoryMetricReader,
) -> None:
    """A non-deadline prune batch failure must surface on a counter.

    The deadline family has ``sweep_timeouts``; an archive
    UniqueViolation-class error used to have NOTHING - the prune loop
    logged, backed off, and retried, and every metric read healthy while
    the drain silently stopped. The counter must name the sweep and fire
    on the failure path, so a silent stoppage can never be invisible again.
    """
    sizer = SweepBatchSizer(default_size=4, divisor=4, failure_threshold=3, window_secs=600.0)
    conn = _WriteViolatesNTimes(fail_times=10**9)

    async def one_attempt() -> None:
        await prune_terminal_jobs(
            conn,  # type: ignore[arg-type]
            retention_per_status={"failed": timedelta(days=1)},
            archive_retention=timedelta(days=30),
            schema="taskq",
            sizer=sizer,
        )

    for _ in range(3):
        with pytest.raises(asyncpg.exceptions.UniqueViolationError):
            await one_attempt()

    assert _unexpected_error_count(unexpected_error_reader, "prune") == 3, (
        "a non-deadline prune batch failure emitted no metric naming it - "
        "the loop logs and retries, so a metrics-only operator watched a "
        "stopped drain read healthy"
    )


async def test_repeated_non_deadline_failures_land_the_drain_at_the_reduced_tier() -> None:
    """The end-to-end shape #358's class of incident needed: repeated
    non-deadline write failures arm the tier, the next attempt runs
    batch_size=1, and the drain completes.

    The sizer is the caller's (the loop keeps one across attempts, the
    latch outlives a failed attempt), so this drives
    ``prune_terminal_jobs`` the way ``_prune_loop`` retries it: three
    failed attempts at the default tier, then the drain converges at the
    reduced one."""
    sizer = SweepBatchSizer(default_size=4, divisor=4, failure_threshold=3, window_secs=600.0)
    conn = _WriteViolatesNTimes(
        fail_times=3,
        write_rows={"failed": [{"actor": "email", "status": "failed", "cnt": 1}]},
    )

    async def one_attempt() -> None:
        await prune_terminal_jobs(
            conn,  # type: ignore[arg-type]
            retention_per_status={"failed": timedelta(days=1)},
            archive_retention=timedelta(days=30),
            schema="taskq",
            sizer=sizer,
        )

    for _ in range(3):
        with pytest.raises(asyncpg.exceptions.UniqueViolationError):
            await one_attempt()

    assert sizer.effective_size() == 1, (
        "three consecutive non-deadline batch failures must latch the "
        "reduced tier (4 // 4) before the next attempt"
    )

    result = await prune_terminal_jobs(
        conn,  # type: ignore[arg-type]
        retention_per_status={"failed": timedelta(days=1)},
        archive_retention=timedelta(days=30),
        schema="taskq",
        sizer=sizer,
    )

    assert conn.candidate_sizes[-1] == 1, (
        f"the completing drain must run its windows at the reduced tier 1, "
        f"ran {[int(s) for s in conn.candidate_sizes]}"
    )
    assert result.total_deleted == 1, (
        "the reduced-tier batch must actually complete: the tier is a "
        "ceiling the drain still drains under, not a stall"
    )

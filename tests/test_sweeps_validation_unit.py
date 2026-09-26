"""Unit-level execution of ``backend/_sweeps.py``'s retention sweeps.

The retention sweeps (`sweep_expired_results`, `sweep_expired_events`,
`sweep_idle_keyed_rows`) previously executed only inside
integration-marked tests, so the SQL *rendering* and the boundary
validation were unpinned at unit level. Against a stub connection this
module runs the real function bodies and pins:

- The rendered statement is BYTE-IDENTICAL to the module constant for a
  vanilla (no-policy-floor) caller. The Timescale policy-floor work
  (branch `feat/timescale-e2e-proof`, commit b31671b2) threads an extra
  retention-floor parameter through these signatures; when it lands, a
  default-argument caller must keep rendering exactly this text, the
  partial-index predicate matching and the measured plan shapes depend
  on it. Regression caught: a floor thread that reformats or re-literals
  the vanilla rendering would silently invalidate the pinned plans
  (tests/test_index_audit.py) and the partial-index matches.
- `sweep_expired_events`' rendered SQL carries the crash-reclaim outbox
  carve-out as a KEEP inside the windowing CTE — the
  ``NOT (kind = 'state_change' AND COALESCE(detail->>'reason','') =
  'lock_expired')`` predicate — and the outbox age-cap arm carries the
  positive (no-COALESCE) form at ``RECLAIM_OUTBOX_RETENTION_MULTIPLIER x
  retention``. Regression caught: dropping or inverting the KEEP
  predicate makes the ordinary retention arm delete unconsumed outbox
  rows, silently losing a crashed worker's reclaim (the poll's
  EventRetentionGapError tripwire never sees the rows); flipping the
  COALESCE away re-NULLs the predicate and exempts nearly the whole
  table from retention.
- The validation raises fire BEFORE any SQL reaches the connection:
  ``timedelta(0)`` retention/horizon (the settings-level disable
  sentinel must never reach a sweep as "delete everything older than
  now"), ``batch_size < 1`` (a silent LIMIT-0 drain stall), and an
  invalid schema identifier (S608 injection boundary).
- ``_apply_batch_statement_timeout`` raises the fail-visible
  RuntimeError when ``current_setting('statement_timeout')`` answers no
  row, and the raise propagates out of ``sweep_expired_locks`` before
  the sweep's own statement runs.

No real PG: the connection is an ``AsyncMock``/stub capturing execute
arguments. The integration tier (tests/test_postgres_sweeps.py,
test_rt_sweeps_*) owns execution semantics; this tier owns rendering and
boundary contracts.
"""

import contextlib
from datetime import timedelta
from unittest.mock import AsyncMock

import pytest

from taskq.backend import _sweeps
from taskq.constants import (
    DEFAULT_EVENT_RETENTION_BATCH_SIZE,
    DEFAULT_EVENT_WRITER_BATCH_SIZE,
    DEFAULT_KEYED_ROW_RECLAIM_BATCH_SIZE,
    RECLAIM_OUTBOX_RETENTION_MULTIPLIER,
)

_SCHEMA = "taskq"


def _mock_conn(tag: str = "UPDATE 0") -> AsyncMock:
    """AsyncMock connection whose ``execute`` returns *tag*.

    ``execute`` is the only surface the retention sweeps use
    (``sweep_expired_locks`` additionally uses ``fetch``/``transaction``,
    stubbed per test).
    """
    conn = AsyncMock()
    conn.execute = AsyncMock(return_value=tag)
    conn.fetch = AsyncMock(return_value=[])
    conn.fetchval = AsyncMock(return_value=None)
    return conn


class _TransactionlessConn:
    """Stub conn for ``sweep_expired_locks``: a transaction that opens,
    and a ``fetch`` whose every answer is *fetch_result*."""

    def __init__(self, fetch_result: list[object]) -> None:
        self._fetch_result = fetch_result
        self.executed: list[str] = []

    def transaction(self) -> object:
        return contextlib.nullcontext()

    async def fetch(self, sql: str, *args: object) -> list[object]:
        # ``sweep_expired_locks`` probes the GUC before its own statement;
        # the probe's answer is the stub's whole script.
        return self._fetch_result

    async def execute(self, sql: str, *args: object) -> str:
        self.executed.append(sql)
        return "UPDATE 0"


# ── Rendering: byte-identical pins (vanilla, no policy floor) ────────────


async def test_sweep_expired_results_renders_the_constant_byte_identically() -> None:
    """The result-TTL sweep executes the module constant formatted with the
    schema and nothing else.

    Regression caught: a signature change (the retention-policy-floor
    threading) that reformats the vanilla rendering — or interpolates a
    floor bound into it — breaks the byte pin here before it can
    invalidate the partial-index predicate matches and plan pins the
    measured derivations rest on.
    """
    conn = _mock_conn(tag="UPDATE 3")

    count = await _sweeps.sweep_expired_results(
        conn,  # pyright: ignore[reportArgumentType]  # Why: AsyncMock duck-types the ConnLike surface under test.
        schema=_SCHEMA,
    )

    assert count == 3
    conn.execute.assert_awaited_once_with(
        _sweeps._SWEEP_RESULT_TTL_SQL.format(schema=_SCHEMA),  # pyright: ignore[reportPrivateUsage]
        DEFAULT_EVENT_WRITER_BATCH_SIZE,
    )


async def test_sweep_expired_events_renders_the_constant_byte_identically() -> None:
    """The event-TTL sweep executes the module constant with only the
    schema and the outbox multiplier interpolated.

    Regression caught: the same vanilla-rendering drift as the sibling
    pin, for the statement that additionally carries the outbox arms —
    the multiplier literal drifting to a floor-conditional form changes
    the outbox age cap the constants module derived against both pinned
    ages.
    """
    conn = _mock_conn(tag="DELETE 7")

    count = await _sweeps.sweep_expired_events(  # pyright: ignore[reportArgumentType]
        conn,
        schema=_SCHEMA,
        retention=timedelta(days=30),
    )

    assert count == 7
    conn.execute.assert_awaited_once_with(
        _sweeps._SWEEP_EVENT_TTL_SQL.format(  # pyright: ignore[reportPrivateUsage]
            schema=_SCHEMA, outbox_multiplier=RECLAIM_OUTBOX_RETENTION_MULTIPLIER
        ),
        timedelta(days=30),
        DEFAULT_EVENT_RETENTION_BATCH_SIZE,
    )


async def test_sweep_idle_keyed_rows_renders_both_arms_byte_identically() -> None:
    """The keyed-row sweep executes BOTH table arms, each the module
    constant, each carrying (horizon, batch_size) in that order.

    Regression caught: an arm added to one table but not the other (the
    sweep's contract is per-table parity), or a re-ordered bind pair
    feeding the horizon into the LIMIT and the batch size into the age
    bound — the latter deletes every keyed row older than ``batch_size
    microseconds``, the dangerous misreading the boundary validation
    exists to prevent.
    """
    conn = _mock_conn(tag="DELETE 1")

    count = await _sweeps.sweep_idle_keyed_rows(
        conn,  # pyright: ignore[reportArgumentType]
        schema=_SCHEMA,
        horizon=timedelta(days=7),
    )

    assert count == 2  # one row per arm's tag
    executed = [call.args[0] for call in conn.execute.await_args_list]
    assert executed == [
        _sweeps._SWEEP_IDLE_KEYED_BUCKETS_SQL.format(schema=_SCHEMA),  # pyright: ignore[reportPrivateUsage]
        _sweeps._SWEEP_IDLE_KEYED_SLOTS_SQL.format(schema=_SCHEMA),  # pyright: ignore[reportPrivateUsage]
    ]
    for call in conn.execute.await_args_list:
        assert call.args[1:] == (timedelta(days=7), DEFAULT_KEYED_ROW_RECLAIM_BATCH_SIZE)


# ── Rendering: the crash-reclaim outbox carve-out ────────────────────────


async def test_event_ttl_sql_keeps_unconsumed_reclaim_outbox_rows() -> None:
    """The windowing CTE's KEEP predicate excludes the crash-reclaim
    outbox slice, and the exclusion sits INSIDE the CTE (the deletable
    set is filtered before the LIMIT).

    Regression caught: deleting the NOT-predicate makes the ordinary
    retention arm victimize unconsumed
    ``kind='state_change' / reason='lock_expired'`` rows — a crashed
    worker's reclaim is silently lost while the watermark advances over
    it, the exact corruption the carve-out exists to prevent. Hoisting
    the exclusion out of the CTE (so the LIMIT sees outbox rows) is the
    under-deletion stall the template comment names: a drain that scans
    LIMIT rows every call yet never deletes.
    """
    sql = _sweeps._SWEEP_EVENT_TTL_SQL.format(  # pyright: ignore[reportPrivateUsage]
        schema=_SCHEMA, outbox_multiplier=RECLAIM_OUTBOX_RETENTION_MULTIPLIER
    )

    keep_predicate = (
        "NOT (kind = 'state_change' AND COALESCE(detail->>'reason', '') = 'lock_expired')"
    )
    assert keep_predicate in sql, "the outbox carve-out must stay a KEEP in the window"
    # Split at the CTE HEADER, not the bare name: the leading comment block
    # mentions "expired_outbox arm" in prose long before the CTE starts.
    cte_body = sql.split("expired_outbox AS MATERIALIZED", 1)[0]
    assert keep_predicate in cte_body, (
        "the KEEP predicate must sit inside the windowing CTE, before the outbox arm"
    )
    # The outbox age-cap arm keeps the POSITIVE (no-COALESCE) form — it is
    # the verbatim partial-index predicate the planner must match.
    assert "WHERE kind = 'state_change' AND (detail->>'reason') = 'lock_expired'" in sql, (
        "the age-cap arm's positive form is the partial-index match contract"
    )
    # The multiplier rides the outbox arm's age bound only.
    assert f"$1::interval * {RECLAIM_OUTBOX_RETENTION_MULTIPLIER}" in sql


# ── Validation: the raises fire before any SQL ───────────────────────────


@pytest.mark.parametrize("sweep", ["results", "events", "idle_keyed"])
@pytest.mark.parametrize("batch_size", [0, -1])
async def test_batch_size_below_one_is_refused_before_any_sql(
    batch_size: int,
    sweep: str,
) -> None:
    """``batch_size`` < 1 is a caller wiring bug (LIMIT 0 is a silent drain
    stall, a negative LIMIT a server-side data error the transient-error
    classification excludes): refused loudly, pre-SQL, on every retention
    sweep.

    Regression caught: a sweep that let 0 through would look like
    "nothing to do" to every caller and metric while the backlog grows
    unbounded.
    """
    conn = _mock_conn()

    with pytest.raises(ValueError, match="batch_size must be >= 1"):
        if sweep == "results":
            await _sweeps.sweep_expired_results(conn, schema=_SCHEMA, batch_size=batch_size)  # pyright: ignore[reportArgumentType]
        elif sweep == "events":
            await _sweeps.sweep_expired_events(
                conn, schema=_SCHEMA, retention=timedelta(days=30), batch_size=batch_size
            )  # pyright: ignore[reportArgumentType]
        else:
            await _sweeps.sweep_idle_keyed_rows(
                conn, schema=_SCHEMA, horizon=timedelta(days=7), batch_size=batch_size
            )  # pyright: ignore[reportArgumentType]

    assert conn.execute.await_count == 0, "the refusal must precede any SQL"


@pytest.mark.parametrize("sweep", ["results", "events", "idle_keyed"])
async def test_invalid_schema_identifier_is_refused_before_any_sql(sweep: str) -> None:
    """A schema that is not a bare SQL identifier (``pg; --`` is the
    injection attempt shape) never reaches ``str.format``.

    Regression caught: the interpolation boundary is the only thing
    between a settings-sourced schema string and a rendered statement.
    """
    conn = _mock_conn()

    with pytest.raises(ValueError, match="invalid schema identifier"):
        if sweep == "results":
            await _sweeps.sweep_expired_results(conn, schema="pg; --")  # pyright: ignore[reportArgumentType]
        elif sweep == "events":
            await _sweeps.sweep_expired_events(conn, schema="pg; --", retention=timedelta(days=30))  # pyright: ignore[reportArgumentType]
        else:
            await _sweeps.sweep_idle_keyed_rows(conn, schema="pg; --", horizon=timedelta(days=7))  # pyright: ignore[reportArgumentType]

    assert conn.execute.await_count == 0, "the refusal must precede any SQL"


async def test_zero_event_retention_is_refused_the_disable_sentinel_is_not_a_sweep_argument() -> (
    None
):
    """``timedelta(0)`` retention is the SETTINGS-level disable sentinel;
    at the sweep boundary it would read as "delete every event older than
    now".

    Regression caught: a WorkerSettings→sweep wiring that forwards the
    sentinel instead of skipping the sweep would erase the whole
    ``job_events`` table one batch at a time, an event-retention
    blackhole that surfaces only as missing narration.
    """
    conn = _mock_conn()

    with pytest.raises(ValueError, match="retention must be positive"):
        await _sweeps.sweep_expired_events(
            conn,  # pyright: ignore[reportArgumentType]
            schema=_SCHEMA,
            retention=timedelta(0),
        )

    assert conn.execute.await_count == 0, "the refusal must precede any SQL"


async def test_zero_keyed_reclaim_horizon_is_refused() -> None:
    """The keyed-row sweep enforces the same positive-horizon contract as
    the event sweep — ``timedelta(0)`` there would read as "delete every
    keyed row older than now", i.e. every fleet-reclaimable bucket and
    slot whose stamp is older than this statement.
    """
    conn = _mock_conn()

    with pytest.raises(ValueError, match="horizon must be positive"):
        await _sweeps.sweep_idle_keyed_rows(
            conn,  # pyright: ignore[reportArgumentType]
            schema=_SCHEMA,
            horizon=timedelta(0),
        )

    assert conn.execute.await_count == 0, "the refusal must precede any SQL"


async def test_negative_event_retention_is_refused() -> None:
    """Negative retention deletes rows in the future; the boundary refuses
    it with the same message contract as the zero sentinel."""
    conn = _mock_conn()

    with pytest.raises(ValueError, match="retention must be positive"):
        await _sweeps.sweep_expired_events(
            conn,  # pyright: ignore[reportArgumentType]
            schema=_SCHEMA,
            retention=timedelta(days=-1),
        )


# ── The statement-timeout probe's fail-visible arm ───────────────────────


async def test_statement_timeout_probe_with_no_row_raises_runtime_error() -> None:
    """``current_setting('statement_timeout')`` is a registered GUC with a
    value in every session; an empty answer means the server replied with
    something the sweep cannot restore, so the arm fails loudly instead
    of guessing a previous value.

    Regression caught: a probe that swallowed the empty row and restored
    e.g. the empty string would silently leave the batch bound applied
    (or the GUC broken) for the caller's subsequent statements in the
    same transaction — the leak test_rt_sweeps_timeout_leak.py pins the
    restore, this pins the refusal to fake one.
    """
    conn = _mock_conn()
    conn.fetch = AsyncMock(return_value=[])

    with pytest.raises(RuntimeError, match="returned no value"):
        await _sweeps._apply_batch_statement_timeout(conn, 5_000)  # pyright: ignore[reportArgumentType,reportPrivateUsage]


async def test_sweep_expired_locks_propagates_the_probe_failure_before_its_own_sql() -> None:
    """In situ: a lock sweep on a conn whose GUC probe answers nothing
    raises the RuntimeError before the sweep statement runs — the batch
    is never half-bound.
    """
    conn = _TransactionlessConn(fetch_result=[])

    with pytest.raises(RuntimeError, match="returned no value"):
        await _sweeps.sweep_expired_locks(  # pyright: ignore[reportArgumentType]
            conn,  # pyright: ignore[reportArgumentType]
            timedelta(seconds=30),
            timedelta(seconds=60),
            schema=_SCHEMA,
        )

    assert conn.executed == [], "the sweep statement must not run on a failed timeout probe"


# ── The probes' happy arms, so the refusals above are not vacuous ────────


async def test_statement_timeout_probe_captures_and_binds() -> None:
    """The probe reads the GUC through ``fetch`` (the complete ConnLike
    duck-type surface — ``fetchval`` is not part of it) and binds the new
    value through ``set_config(..., true)``, returning the previous value
    for the restore.
    """
    conn = _mock_conn()
    conn.fetch = AsyncMock(return_value=[{"current_setting": "30s"}])

    prev = await _sweeps._apply_batch_statement_timeout(conn, 5_000)  # pyright: ignore[reportPrivateUsage]

    assert prev == "30s"
    conn.execute.assert_awaited_once_with(
        "SELECT set_config('statement_timeout', $1, true)", "5000"
    )

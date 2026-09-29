# ruff: noqa: S608  # Why: schema is a fixture-derived test identifier, not user input; every value is $-bound.
"""Tests for the ``taskq insights`` operator CLI surface.

``taskq.insights`` (the pure-read SQL layer, test_insights.py) answers the
operator questions; these tests pin the terminal surface that renders its
rows: ``taskq insights [wait|balance|drain|cron|all]``.

The contract under test:

* every surface renders its module rows as an operator-readable table and
  exits 0 with something to report or a named nothing-to-report (the
  doctor's read-only discipline: SELECT-only statements, exit 0 always);
* the confounds the SQL layer documents are carried into the terminal:
  a deferred wait row is LABELED (reschedules excluded from the clean
  percentiles), a no-traffic drain renders the honest
  "no traffic in window" caveat and NEVER an eta of 0, an
  over-threshold balance row renders a marker, and a runaway cron
  schedule renders the trending verdict;
* ``--help`` documents every column's meaning;
* ``--window`` accepts only the module's ``INSIGHTS_WINDOWS`` closed set,
  ``--actor`` is the wait surface's grouping switch, and the queue filter
  is refused where the surface has no queue dimension.

Unit tier: the asyncpg connection is monkeypatched at the ``taskq.cli``
import boundary (the established seam, tests/test_cli_queues.py); the real
insights fetchers run against the fake connection, so the rendering and
error paths are pinned end-to-end while Postgres behavior stays out of
scope. The integration tier at the bottom runs the same commands against a
real migrated Postgres container with a seeded scenario.
"""

from collections.abc import AsyncIterator, Iterator, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from typer.testing import CliRunner

from taskq._ids import new_uuid
from taskq.cli import app
from taskq.migrate import apply_pending
from taskq.testing._shared_containers import creator_labels, skip_test_without_docker
from taskq.testing.assertions import plain_cli_output

runner = CliRunner()

# The doctor's read-only discipline, restated for this command family: any
# writing verb in any statement the command issued is a failure.
_WRITE_VERBS = ("insert", "update", "delete", "truncate", "drop", "alter", "create")


class _FakeConn:
    """asyncpg.Connection stand-in answering the insights statements.

    The fetchers of ``taskq.insights`` are the only data path (the CLI
    writes no SQL of its own), so the fake dispatches on the statement's
    distinctive aggregate — the same row shapes the real statements return.
    """

    def __init__(
        self,
        *,
        wait_rows: list[dict[str, Any]] | None = None,
        balance_rows: list[dict[str, Any]] | None = None,
        drain_rows: list[dict[str, Any]] | None = None,
        cron_rows: list[dict[str, Any]] | None = None,
    ) -> None:
        self.executed: list[str] = []
        self.written: list[str] = []
        self._wait_rows = wait_rows or []
        self._balance_rows = balance_rows or []
        self._drain_rows = drain_rows or []
        self._cron_rows = cron_rows or []

    async def fetch(self, query: str, *args: object) -> list[dict[str, Any]]:
        self.executed.append(query)
        if "percentile_cont" in query:
            return list(self._wait_rows)
        if "effective_capacity" in query:
            return list(self._balance_rows)
        if "eta_seconds" in query:
            return list(self._drain_rows)
        if "runaway_trending" in query:
            return list(self._cron_rows)
        raise AssertionError(f"unexpected statement handed to the connection: {query[:120]!r}")

    async def execute(self, query: str, *args: object) -> Any:
        # The write-trap: the read-only contract makes `fetch` the ONLY data
        # method the command may call. Recording before raising keeps a
        # failing test's evidence readable; the raise (or the empty
        # `written` assertion) is what reds a command that starts writing.
        self.written.append(query)
        raise AssertionError(
            f"insights issued a non-fetch statement (read-only contract break): {query[:120]!r}"
        )

    async def executemany(self, query: str, args: object) -> Any:
        self.written.append(query)
        raise AssertionError(
            f"insights issued a non-fetch statement (read-only contract break): {query[:120]!r}"
        )

    async def close(self) -> None: ...


def _patch_conn(monkeypatch: pytest.MonkeyPatch, conn: _FakeConn) -> None:
    async def fake_connect(dsn: str, *args: object, **kwargs: object) -> Any:
        return conn

    monkeypatch.setattr("taskq.cli.asyncpg.connect", fake_connect)


def _wait_row(
    queue: str = "email",
    segment: str = "clean",
    count: int = 10,
    p50: float = 0.4,
    p95: float = 12.5,
    max_s: float = 60.0,
) -> dict[str, Any]:
    return {
        "queue": queue,
        "segment": segment,
        "count": count,
        "p50_wait_s": p50,
        "p95_wait_s": p95,
        "max_wait_s": max_s,
    }


def _balance_row(
    queue: str = "email",
    *,
    depth: int = 25,
    live_workers: int = 2,
    effective_capacity: int = 10,
    utilization: float | None = 2.5,
    oldest_due_age_s: float | None = 900.0,
) -> dict[str, Any]:
    return {
        "queue": queue,
        "depth": depth,
        "oldest_due_at": datetime.now(UTC),
        "oldest_due_age_s": oldest_due_age_s,
        "scheduled_depth": 0,
        "wave_min_scheduled_at": None,
        "wave_max_scheduled_at": None,
        "live_workers": live_workers,
        "actor_capacity": 5,
        "effective_capacity": effective_capacity,
        "utilization": utilization,
    }


def _drain_row(
    queue: str = "email",
    *,
    has_traffic: bool = True,
    eta_seconds: float | None = 240.0,
    depth: int = 120,
) -> dict[str, Any]:
    return {
        "queue": queue,
        "depth": depth,
        "terminalisations": 0 if not has_traffic else 30,
        "completions_per_second": 0.0 if not has_traffic else 0.5,
        "has_traffic": has_traffic,
        "eta_seconds": eta_seconds,
        "scheduled_depth": 0,
        "wave_min_scheduled_at": None,
        "wave_max_scheduled_at": None,
    }


def _cron_row(
    *,
    actor: str = "ticker",
    fires_window: int = 6,
    cleared_window: int = 1,
    fires_prior: int = 1,
    cleared_prior: int = 0,
    outstanding: int = 6,
    runaway_trending: bool = True,
) -> dict[str, Any]:
    return {
        "schedule_id": new_uuid(),
        "actor": actor,
        "cron_expr": "* * * * *",
        "timezone": "UTC",
        "dst_strategy": "skip",
        "enabled": True,
        "fires_window": fires_window,
        "cleared_window": cleared_window,
        "fires_prior": fires_prior,
        "cleared_prior": cleared_prior,
        "outstanding": outstanding,
        "runaway_trending": runaway_trending,
    }


# ── wait ─────────────────────────────────────────────────────────────────


def test_wait_renders_each_segment_labeled(monkeypatch: pytest.MonkeyPatch) -> None:
    """The clean/deferred split is the surface's point: every segment row
    must carry its label, and the deferred rows ride beside the clean ones,
    not folded into them."""
    _patch_conn(
        monkeypatch,
        _FakeConn(
            wait_rows=[
                _wait_row("email", "clean", 10, 0.4, 12.5, 60.0),
                _wait_row("email", "deferred", 3, 0.2, 1.0, 2.0),
            ]
        ),
    )

    result = runner.invoke(app, ["insights", "wait"])

    assert result.exit_code == 0
    plain = plain_cli_output(result.output)
    assert "email" in plain
    assert "clean" in plain
    assert "deferred" in plain
    assert "10" in plain and "3" in plain


def test_wait_help_says_reschedules_are_excluded_from_clean() -> None:
    """The deferred label is only honest if --help says what it excludes:
    reschedules are not in the clean percentiles, and a deferred row's
    wait measures only the final leg."""
    result = runner.invoke(app, ["insights", "--help"])

    assert result.exit_code == 0
    plain = plain_cli_output(result.output).lower()
    assert "deferred" in plain
    assert "clean" in plain
    assert "reschedule" in plain, (
        "--help must say that reschedules are excluded from the clean "
        "percentiles — the deferred label means nothing without it"
    )


def test_wait_actor_flag_switches_to_the_per_actor_grouping(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """--actor is a grouping switch, not a post-filter alone: the statement
    must be the module's per-(actor, queue) shape, and only that actor's
    rows render."""
    conn = _FakeConn(
        wait_rows=[
            _wait_row("email", "clean", 10, 0.4, 12.5, 60.0),
            _wait_row("batch", "clean", 5, 0.1, 0.9, 3.0),
        ]
    )
    # Per-actor rows carry the actor key.
    for row in conn._wait_rows:
        row["actor"] = "mailer" if row["queue"] == "email" else "other"
    _patch_conn(monkeypatch, conn)

    result = runner.invoke(app, ["insights", "wait", "--actor", "mailer"])

    assert result.exit_code == 0
    assert any("u.actor" in q for q in conn.executed), (
        "the --actor read must run the module's per-(actor, queue) grouping, "
        "not a hand-rolled query"
    )
    plain = plain_cli_output(result.output)
    assert "mailer" in plain
    assert "other" not in plain, "rows for other actors must be filtered out"


def test_wait_empty_window_reports_nothing_to_report(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A window with no terminal activity is the quiet state, not an error:
    exit 0 with the fact named (the doctor's read-only discipline)."""
    _patch_conn(monkeypatch, _FakeConn(wait_rows=[]))

    result = runner.invoke(app, ["insights", "wait"])

    assert result.exit_code == 0
    assert "nothing to report" in plain_cli_output(result.output)


# ── balance ──────────────────────────────────────────────────────────────


def test_balance_over_threshold_row_renders_a_marker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """BEHAVIORAL (red-first): a queue whose utilization exceeds 1 is the
    starved shape; its row must be MARKED, not silently presented among
    the healthy ones."""
    _patch_conn(
        monkeypatch,
        _FakeConn(
            balance_rows=[
                _balance_row("email", utilization=2.5),
                _balance_row(
                    "default", depth=1, live_workers=2, effective_capacity=10, utilization=0.1
                ),
            ]
        ),
    )

    result = runner.invoke(app, ["insights", "balance"])

    assert result.exit_code == 0
    plain = plain_cli_output(result.output)
    assert "!!" in plain, "an over-threshold row must render a visible marker"
    # The marker rides the offending row, not the healthy one: the marked
    # line names the starved queue.
    assert "email" in plain


def test_balance_starved_row_renders_the_no_capacity_marker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """NULL utilization is the starvation shape the module documents
    (nothing can serve the queue); it must be marked too, never rendered
    as a blank or a 0."""
    _patch_conn(
        monkeypatch,
        _FakeConn(
            balance_rows=[
                _balance_row("idle", live_workers=0, effective_capacity=0, utilization=None)
            ]
        ),
    )

    result = runner.invoke(app, ["insights", "balance"])

    assert result.exit_code == 0
    plain = plain_cli_output(result.output)
    assert "!!" in plain
    assert "capacity" in plain


def test_balance_healthy_row_renders_no_marker(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_conn(
        monkeypatch,
        _FakeConn(
            balance_rows=[
                _balance_row(
                    "default", depth=2, live_workers=1, effective_capacity=5, utilization=0.4
                )
            ]
        ),
    )

    result = runner.invoke(app, ["insights", "balance"])

    assert result.exit_code == 0
    assert "!!" not in plain_cli_output(result.output)


def test_balance_renders_the_utilization_and_capacity_columns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_conn(
        monkeypatch,
        _FakeConn(
            balance_rows=[
                _balance_row(
                    "email",
                    depth=25,
                    live_workers=2,
                    effective_capacity=10,
                    utilization=2.5,
                    oldest_due_age_s=900.0,
                )
            ]
        ),
    )

    result = runner.invoke(app, ["insights", "balance"])

    assert result.exit_code == 0
    plain = plain_cli_output(result.output)
    for column_value in ("25", "2", "10", "2.50"):
        assert column_value in plain, f"column value {column_value!r} must render"


# ── drain ────────────────────────────────────────────────────────────────


def test_drain_no_traffic_renders_the_honest_string_never_eta_zero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """BEHAVIORAL (red-first — red on eta=0): a window with no
    terminalisations has NO estimate. The honest caveat is the string the
    module documents, never a 0 that would read as 'already drained'."""
    _patch_conn(
        monkeypatch,
        _FakeConn(drain_rows=[_drain_row("email", has_traffic=False, eta_seconds=None, depth=40)]),
    )

    result = runner.invoke(app, ["insights", "drain"])

    assert result.exit_code == 0
    plain = plain_cli_output(result.output)
    assert "no traffic in window" in plain
    assert "0s" not in plain, "a no-traffic queue must never render an eta of 0"


def test_drain_with_traffic_renders_a_human_eta(monkeypatch: pytest.MonkeyPatch) -> None:
    """240s of eta reads as '4m0s', not a bare float of seconds."""
    _patch_conn(
        monkeypatch,
        _FakeConn(drain_rows=[_drain_row("email", has_traffic=True, eta_seconds=240.0, depth=120)]),
    )

    result = runner.invoke(app, ["insights", "drain"])

    assert result.exit_code == 0
    plain = plain_cli_output(result.output)
    assert "4m0s" in plain
    assert "240.0" not in plain and "240s" not in plain


# ── cron ─────────────────────────────────────────────────────────────────


def test_cron_runaway_renders_the_trending_verdict(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_conn(monkeypatch, _FakeConn(cron_rows=[_cron_row(runaway_trending=True)]))

    result = runner.invoke(app, ["insights", "cron"])

    assert result.exit_code == 0
    plain = plain_cli_output(result.output)
    assert "runaway" in plain
    assert "!!" in plain


def test_cron_healthy_schedule_renders_no_runaway(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_conn(
        monkeypatch,
        _FakeConn(
            cron_rows=[
                _cron_row(
                    fires_window=5,
                    cleared_window=5,
                    fires_prior=5,
                    cleared_prior=5,
                    outstanding=0,
                    runaway_trending=False,
                )
            ]
        ),
    )

    result = runner.invoke(app, ["insights", "cron"])

    assert result.exit_code == 0
    assert "runaway" not in plain_cli_output(result.output)


def test_cron_renders_fires_cleared_and_outstanding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_conn(
        monkeypatch,
        _FakeConn(
            cron_rows=[
                _cron_row(fires_window=6, cleared_window=1, outstanding=6, runaway_trending=True)
            ]
        ),
    )

    result = runner.invoke(app, ["insights", "cron"])

    assert result.exit_code == 0
    plain = plain_cli_output(result.output)
    assert "ticker" in plain
    for value in ("6", "1"):
        assert value in plain


# ── all (the default surface) ────────────────────────────────────────────


def test_default_surface_is_all_and_renders_every_section(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = _FakeConn(
        wait_rows=[_wait_row()],
        balance_rows=[_balance_row()],
        drain_rows=[_drain_row()],
        cron_rows=[_cron_row()],
    )
    _patch_conn(monkeypatch, conn)

    result = runner.invoke(app, ["insights"])

    assert result.exit_code == 0
    plain = plain_cli_output(result.output)
    for section in ("wait", "balance", "drain", "cron"):
        assert section in plain, f"the default surface must render the {section} section"
    # One connection, four module reads: every surface's statement ran.
    assert len(conn.executed) == 4


def test_explicit_all_matches_the_default(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = _FakeConn(
        wait_rows=[_wait_row()],
        balance_rows=[_balance_row()],
        drain_rows=[_drain_row()],
        cron_rows=[_cron_row()],
    )
    _patch_conn(monkeypatch, conn)

    result = runner.invoke(app, ["insights", "all"])

    assert result.exit_code == 0
    assert len(conn.executed) == 4


# ── the option grammar ───────────────────────────────────────────────────


def test_invalid_surface_exits_1() -> None:
    result = runner.invoke(app, ["insights", "extrapolate"])

    assert result.exit_code == 1
    assert "wait" in plain_cli_output(result.output), "the refusal must name the valid surfaces"


def test_invalid_window_exits_1_naming_the_closed_set(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = _FakeConn()
    _patch_conn(monkeypatch, conn)

    result = runner.invoke(app, ["insights", "wait", "--window", "30d"])

    assert result.exit_code == 1
    assert "1h" in plain_cli_output(result.output), "the refusal must name the valid windows"
    assert conn.executed == [], "the guard must fire before any statement is issued"


def test_actor_outside_wait_is_refused(monkeypatch: pytest.MonkeyPatch) -> None:
    """--actor is the wait surface's grouping switch; accepting it silently
    on balance (where it can change nothing) would be a lying option."""
    conn = _FakeConn(balance_rows=[_balance_row()])
    _patch_conn(monkeypatch, conn)

    result = runner.invoke(app, ["insights", "balance", "--actor", "mailer"])

    assert result.exit_code == 1
    assert conn.executed == [], "the guard must fire before any statement is issued"


def test_queue_on_cron_is_refused(monkeypatch: pytest.MonkeyPatch) -> None:
    """The fan-out ledger is per schedule and has no queue dimension."""
    conn = _FakeConn(cron_rows=[_cron_row()])
    _patch_conn(monkeypatch, conn)

    result = runner.invoke(app, ["insights", "cron", "--queue", "email"])

    assert result.exit_code == 1
    assert conn.executed == [], "the guard must fire before any statement is issued"


def test_queue_flag_filters_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_conn(
        monkeypatch,
        _FakeConn(
            wait_rows=[_wait_row("email", "clean"), _wait_row("batch", "clean")],
            balance_rows=[_balance_row("email"), _balance_row("batch")],
            drain_rows=[_drain_row("email"), _drain_row("batch")],
        ),
    )

    result = runner.invoke(app, ["insights", "wait", "--queue", "email"])

    assert result.exit_code == 0
    plain = plain_cli_output(result.output)
    assert "email" in plain
    assert "batch" not in plain


def test_help_documents_every_column() -> None:
    """The command's own help is the operator's data dictionary: every
    rendered column's meaning (and the confounds) must be stated there."""
    result = runner.invoke(app, ["insights", "--help"])

    assert result.exit_code == 0
    plain = plain_cli_output(result.output).lower()
    for phrase in (
        # wait columns
        "count",
        "p50",
        "p95",
        "max",
        # balance columns
        "depth",
        "live",
        "effective capacity",
        "utilization",
        "oldest",
        # drain
        "eta",
        # cron columns
        "fires",
        "cleared",
        "outstanding",
        "runaway",
    ):
        assert phrase in plain, f"--help must document the column/term {phrase!r}"


def test_every_statement_is_read_only(monkeypatch: pytest.MonkeyPatch) -> None:
    """The doctor's read-only discipline: the command is safe to run
    against production mid-incident, so every statement it issues is a
    SELECT."""
    conn = _FakeConn(
        wait_rows=[_wait_row()],
        balance_rows=[_balance_row()],
        drain_rows=[_drain_row()],
        cron_rows=[_cron_row()],
    )
    _patch_conn(monkeypatch, conn)

    result = runner.invoke(app, ["insights"])

    assert result.exit_code == 0
    assert conn.executed, "the default surface must issue the module reads"
    for query in conn.executed:
        lowered = query.lower()
        for verb in _WRITE_VERBS:
            assert f"{verb} " not in lowered, (
                f"insights issued a writing statement ({verb}); the command "
                "must stay read-only like doctor"
            )


# ── the read-only contract, restated at the connection boundary ──────────


def test_no_write_ever_reaches_the_connection(monkeypatch: pytest.MonkeyPatch) -> None:
    """BEHAVIORAL (red-first): the pin is the CONNECTION, not the statement
    text. `fetch` is the only data method the command may call — the
    write-trap `execute`/`executemany` methods on the fake red the first
    invocation that reaches for one, even a write hiding in a statement
    the verb scan cannot parse."""
    conn = _FakeConn(
        wait_rows=[_wait_row()],
        balance_rows=[_balance_row()],
        drain_rows=[_drain_row()],
        cron_rows=[_cron_row()],
    )
    _patch_conn(monkeypatch, conn)

    result = runner.invoke(app, ["insights", "all"])

    assert conn.written == [], (
        "insights issued a non-fetch statement; the command must stay read-only"
    )
    assert result.exit_code == 0


def test_no_write_rides_the_all_surface_before_its_tables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A write smuggled in BEFORE or BETWEEN the reads (a setup statement on
    the shared connection) is still a write. Every statement the whole
    `all` surface issues — fetch and otherwise — must be in `executed`,
    none in `written`."""
    conn = _FakeConn(
        wait_rows=[_wait_row()],
        balance_rows=[_balance_row()],
        drain_rows=[_drain_row()],
        cron_rows=[_cron_row()],
    )
    _patch_conn(monkeypatch, conn)

    result = runner.invoke(app, ["insights", "all"])

    assert result.exit_code == 0
    assert len(conn.executed) == 4, "each surface's module read must run on the one connection"
    assert conn.written == []


# ── the boundary discipline: guard ORDER, not just guard presence ───────


def test_guards_fire_before_any_connection_is_opened(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """BEHAVIORAL (red-first): the refusals must happen before
    ``asyncpg.connect`` is even ATTEMPTED — mid-incident, an unauthorized
    option combination must not open a socket to the fleet at all. The
    existing pins assert no statement ran; this one asserts no
    connection was opened, and that the message is the guard's (not a
    settings-load or connection crash that merely also exits 1)."""
    connects: list[str] = []

    async def recording_connect(dsn: str, *args: object, **kwargs: object) -> Any:
        connects.append(dsn)
        return _FakeConn()

    monkeypatch.setattr("taskq.cli.asyncpg.connect", recording_connect)
    for argv, refusal in (
        (["insights", "extrapolate"], "unknown insights surface"),
        (["insights", "wait", "--window", "30d"], "invalid --window"),
        (["insights", "balance", "--actor", "mailer"], "--actor applies to the wait surface"),
        (["insights", "cron", "--queue", "email"], "--queue does not apply to the cron surface"),
    ):
        result = runner.invoke(app, argv)
        assert result.exit_code == 1, f"{argv} must be refused"
        assert refusal in plain_cli_output(result.output), (
            f"{argv} must be refused by its own guard, not by a later crash"
        )
        assert connects == [], f"{argv} must refuse before opening a connection"


def test_invalid_window_is_refused_not_coerced(monkeypatch: pytest.MonkeyPatch) -> None:
    """A window outside the module's closed set must be REFUSED with the
    valid set named — never silently coerced to a nearest neighbor (a
    coerced window changes the numbers the operator is reading without
    changing the command they typed)."""
    conn = _FakeConn()
    _patch_conn(monkeypatch, conn)

    for window in ("30d", "1w", "1h ", "", "1H"):
        result = runner.invoke(app, ["insights", "wait", "--window", window])

        assert result.exit_code == 1, f"--window {window!r} must be refused"
        assert "expected one of" in plain_cli_output(result.output)
        assert conn.executed == []


def test_schema_guard_fires_before_the_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The queue_ops convention this command claims: the schema identifier
    is re-checked at the boundary, and the check precedes
    ``asyncpg.connect`` — a hostile schema name must not open a socket
    at all (the settings layer refuses it at load; this is the
    defence-in-depth re-check, pinned in its ORDER)."""
    connects: list[str] = []

    async def recording_connect(dsn: str, *args: object, **kwargs: object) -> Any:
        connects.append(dsn)
        return _FakeConn()

    monkeypatch.setattr("taskq.cli.asyncpg.connect", recording_connect)

    class _HostileSchemaSettings:
        pg_dsn = "postgresql://irrelevant"
        schema_name = 'bad"; DROP SCHEMA x'

    class _HostileSettingsClass:
        @staticmethod
        def load() -> Any:
            return _HostileSchemaSettings()

    monkeypatch.setattr("taskq.cli.TaskQSettings", _HostileSettingsClass)

    result = runner.invoke(app, ["insights", "wait"])

    assert result.exit_code == 1
    assert "invalid schema name" in plain_cli_output(result.output)
    assert connects == [], "the schema re-check must fire before the connection is opened"


def test_metacharacter_actor_and_queue_values_never_reach_sql(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """--actor/--queue are Python post-filters over the module's returned
    rows, never SQL fragments: a value carrying metacharacters cannot
    inject, the issued statements stay byte-identical to the module's
    own templates, and the command still exits 0 (the filter just keeps
    nothing). The value must not appear in ANY issued statement."""
    hostile = "x'; DROP TABLE jobs; --"
    conn = _FakeConn(wait_rows=[_wait_row("email", "clean")])
    conn._wait_rows[0]["actor"] = "x"  # the per-actor grouping carries the actor key
    _patch_conn(monkeypatch, conn)

    result = runner.invoke(app, ["insights", "wait", "--actor", hostile, "--queue", hostile])

    assert result.exit_code == 0
    assert conn.written == []
    assert conn.executed, "the module's read must still run"
    for query in conn.executed:
        assert hostile not in query, "a filter value must never be interpolated into SQL"
        lowered = query.lower()
        for verb in _WRITE_VERBS:
            assert f"{verb} " not in lowered


# ── the renderer's honesty under edge data ───────────────────────────────


def test_wait_null_percentiles_render_dash_never_zero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A (queue, segment) group whose percentiles came back NULL must
    render '-' in the p50/p95/max cells — a 0 would read as 'instant',
    the exact lie the duration formatter's docstring forbids."""
    row = _wait_row("email", "clean", 1)
    row["p50_wait_s"] = None
    row["p95_wait_s"] = None
    row["max_wait_s"] = None
    _patch_conn(monkeypatch, _FakeConn(wait_rows=[row]))

    result = runner.invoke(app, ["insights", "wait"])

    assert result.exit_code == 0
    line = next(line for line in result.output.splitlines() if "clean" in line)
    cells = line.split()
    assert cells[-3:] == ["-", "-", "-"], "NULL percentiles must render '-', not 0"
    assert "0s" not in line


def test_drain_negative_eta_keeps_its_sign(monkeypatch: pytest.MonkeyPatch) -> None:
    """An eta that arrives negative (a pathological row) must not be
    laundered into a healthy-looking positive or a crash: the sign
    survives into the cell, so the operator sees the pathology."""
    _patch_conn(
        monkeypatch,
        _FakeConn(drain_rows=[_drain_row("email", has_traffic=True, eta_seconds=-30.0, depth=1)]),
    )

    result = runner.invoke(app, ["insights", "drain"])

    assert result.exit_code == 0
    line = next(line for line in result.output.splitlines() if "email" in line)
    assert line.split()[-1] == "-30.0s", "the negative eta must keep its sign, not read as drained"


def test_drain_absurd_eta_stays_readable(monkeypatch: pytest.MonkeyPatch) -> None:
    """A 31-year eta must render as a human duration, never scientific
    notation or a raw float the operator cannot read at a glance."""
    _patch_conn(
        monkeypatch,
        _FakeConn(
            drain_rows=[
                _drain_row("email", has_traffic=True, eta_seconds=1_000_000_000.0, depth=10**9)
            ]
        ),
    )

    result = runner.invoke(app, ["insights", "drain"])

    assert result.exit_code == 0
    line = next(line for line in result.output.splitlines() if "email" in line)
    assert line.split()[-1] == "11574d1h"
    assert "e+" not in line and "1e" not in line


def test_cron_runaway_verdict_is_the_exact_marker(monkeypatch: pytest.MonkeyPatch) -> None:
    """Fires with ZERO cleared across BOTH windows is the maximal runaway
    shape: the verdict cell must be the exact documented marker."""
    _patch_conn(
        monkeypatch,
        _FakeConn(
            cron_rows=[
                _cron_row(
                    fires_window=3,
                    cleared_window=0,
                    fires_prior=3,
                    cleared_prior=0,
                    outstanding=3,
                    runaway_trending=True,
                )
            ]
        ),
    )

    result = runner.invoke(app, ["insights", "cron"])

    assert result.exit_code == 0
    line = next(line for line in result.output.splitlines() if "ticker" in line)
    assert line.split()[-3:] == ["!!", "runaway", "trending"]


def test_cron_one_window_burst_is_not_the_runaway_verdict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fires outrunning clearance in the CURRENT window only is a burst,
    not a trend — the module's two-window rule says the verdict must be
    the plain 'ok', never the runaway marker."""
    _patch_conn(
        monkeypatch,
        _FakeConn(
            cron_rows=[
                _cron_row(
                    fires_window=5,
                    cleared_window=0,
                    fires_prior=0,
                    cleared_prior=0,
                    outstanding=5,
                    runaway_trending=False,
                )
            ]
        ),
    )

    result = runner.invoke(app, ["insights", "cron"])

    assert result.exit_code == 0
    line = next(line for line in result.output.splitlines() if "ticker" in line)
    assert line.split()[-1] == "ok"
    assert "runaway" not in plain_cli_output(result.output)


def test_cron_schedule_id_renders_shortened_not_mangled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """--help documents the schedule column as 'the schedule id,
    shortened': the rendered cell is the id's first 8 chars, never a
    bare float-like reinterpretation or the full 36-char uuid."""
    schedule_id = new_uuid()
    row = _cron_row()
    row["schedule_id"] = schedule_id
    _patch_conn(monkeypatch, _FakeConn(cron_rows=[row]))

    result = runner.invoke(app, ["insights", "cron"])

    assert result.exit_code == 0
    assert str(schedule_id)[:8] in plain_cli_output(result.output)
    assert str(schedule_id) not in plain_cli_output(result.output)


def test_balance_flag_column_renders_each_label_exactly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """All three flag states are part of the contract, each exact: the
    over-threshold marker, the no-capacity marker, and the healthy row's
    EMPTY flag cell (a healthy queue must not inherit a stray '!!' from
    a neighboring row's marker)."""
    _patch_conn(
        monkeypatch,
        _FakeConn(
            balance_rows=[
                _balance_row("over", utilization=2.5),
                _balance_row("idle", live_workers=0, effective_capacity=0, utilization=None),
                _balance_row("ok", depth=1, live_workers=1, effective_capacity=2, utilization=0.5),
            ]
        ),
    )

    result = runner.invoke(app, ["insights", "balance"])

    assert result.exit_code == 0
    over_line = next(line for line in result.output.splitlines() if line.startswith("over"))
    idle_line = next(line for line in result.output.splitlines() if line.startswith("idle"))
    ok_line = next(line for line in result.output.splitlines() if line.startswith("ok"))
    assert over_line.split()[-3:] == ["!!", "over", "threshold"]
    assert idle_line.split()[-3:] == ["!!", "no", "capacity"]
    assert "!!" not in ok_line, "a healthy row's flag cell must be empty"


def test_unicode_and_long_names_stay_readable(monkeypatch: pytest.MonkeyPatch) -> None:
    """A queue name that is unicode-heavy and very long must render
    verbatim (never truncated, escaped, or mangled) and the table must
    stay aligned: every line the table renders is padded to one
    code-point width, so the columns still line up."""
    long_queue = "排隊-émail-🔥-" + "q" * 60
    unicode_actor = "actor-ß-名前"
    row = _wait_row(long_queue, "clean")
    row["actor"] = unicode_actor
    _patch_conn(monkeypatch, _FakeConn(wait_rows=[row]))

    result = runner.invoke(app, ["insights", "wait", "--actor", unicode_actor])

    assert result.exit_code == 0
    assert long_queue in plain_cli_output(result.output), "the queue name must render verbatim"
    table_lines = [
        line for line in result.output.splitlines() if line.startswith(("queue", long_queue))
    ]
    assert len(table_lines) == 2, "the header and the data row must both be found"
    assert len({len(line) for line in table_lines}) == 1, (
        "the table's lines must all pad to the same width (aligned columns)"
    )


# ── the help contract: a drift pin ───────────────────────────────────────


def test_help_contract_covers_every_rendered_column_and_marker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """BEHAVIORAL (red-first): --help is the operator's column dictionary,
    so it is pinned against what the renderers ACTUALLY render, not
    against a hand-copied list. Every header each surface renders (and
    every marker string the renderers emit) must appear in --help —
    adding a column or marker without documenting it reds this test."""
    captured: list[list[str]] = []

    def spy(headers: Sequence[str], rows: Sequence[Sequence[str]]) -> None:
        captured.append(list(headers))

    monkeypatch.setattr("taskq.cli._insights_table", spy)
    for argv in (
        ["insights", "wait"],
        ["insights", "wait", "--actor", "mailer"],
        ["insights", "balance"],
        ["insights", "drain"],
        ["insights", "cron"],
    ):
        conn = _FakeConn(
            wait_rows=[_wait_row()],
            balance_rows=[_balance_row()],
            drain_rows=[_drain_row()],
            cron_rows=[_cron_row()],
        )
        conn._wait_rows[0]["actor"] = "mailer"  # the per-actor grouping carries the key
        _patch_conn(monkeypatch, conn)
        result = runner.invoke(app, argv)
        assert result.exit_code == 0, result.output

    assert captured, "every surface must render a table to capture headers from"
    help_result = runner.invoke(app, ["insights", "--help"])
    assert help_result.exit_code == 0
    # CI's environment makes rich colorize the help even without a TTY
    # (the run-36369327985 lesson: the words arrive interleaved with
    # \x1b[..m escapes, and a whitespace-only normalization still misses
    # every marker). Presentation is not contract: strip the FULL escape
    # grammar (SGR, private-mode CSI, two-byte escapes, OSC) through the
    # shared idiom before matching.
    plain = plain_cli_output(help_result.output).lower()
    for headers in captured:
        for header in headers:
            for token in header.lower().split("_"):
                assert token in plain, (
                    f"column {header!r} renders but --help does not document "
                    f"it ({token!r} missing) — the help contract drifted"
                )
    for marker in (
        "!! no capacity",
        "!! over threshold",
        "!! runaway trending",
        "no traffic in window",
    ):
        assert marker in plain, f"the renderer emits {marker!r} but --help does not document it"


# ── integration tier: the real SQL path on a real container ─────────────


@pytest.fixture(scope="module")
def pg_container() -> Iterator[Any]:
    skip_test_without_docker()
    from testcontainers.community.postgres import PostgresContainer

    with PostgresContainer(
        image="postgres:18", username="taskq", password="taskq", dbname="taskq"
    ).with_kwargs(labels=creator_labels()) as container:
        yield container


@pytest.fixture(scope="module")
def pg_dsn(pg_container: Any) -> str:
    return pg_container.get_connection_url().replace("postgresql+psycopg2://", "postgresql://")


@pytest.fixture(scope="module")
async def pg_env(pg_dsn: str) -> AsyncIterator[dict[str, str]]:
    """A migrated schema with the scenario the surfaces read: a busy queue
    (clean + deferred traffic, a worker, capacity, a due backlog), an idle
    queue (due work, no worker, no traffic), and one cron schedule."""
    import asyncpg

    conn = await asyncpg.connect(pg_dsn)
    schema = "insights_cli"
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema)
        now = datetime.now(UTC)

        # The busy queue: two clean terminal jobs, one deferred terminal
        # job, one due pending job.
        for wait_s, snooze in ((60.0, 0), (120.0, 0), (5.0, 1)):
            started = now - timedelta(seconds=30)
            scheduled = started - timedelta(seconds=wait_s)
            await conn.execute(
                f"""INSERT INTO {schema}.jobs (
                        id, actor, queue, payload, max_attempts, retry_kind, status,
                        created_at, scheduled_at, started_at, finished_at,
                        snooze_count, rate_limit_blocked_count
                    ) VALUES (
                        $1, 'mailer', 'email', '{{"v": 1}}'::jsonb, 3, 'transient',
                        $2::{schema}.job_status, $3, $4, $5, $6, $7, 0
                    )""",
                new_uuid(),
                "succeeded",
                scheduled,
                scheduled,
                started,
                started + timedelta(seconds=1),
                snooze,
            )
        # The due backlog the balance/drain reads measure.
        await conn.execute(
            f"""INSERT INTO {schema}.jobs (
                    id, actor, queue, payload, max_attempts, retry_kind, status, scheduled_at
                ) VALUES (
                    $1, 'mailer', 'email', '{{"v": 1}}'::jsonb, 3, 'transient',
                    $2::{schema}.job_status, statement_timestamp() - interval '15 minutes'
                )""",
            new_uuid(),
            "pending",
        )
        # A live worker serving email, and the actor capacity for it.
        await conn.execute(
            f"""INSERT INTO {schema}.workers (id, hostname, pid, queues, last_seen_at)
                VALUES ($1, 'cli-test-host', 1, ARRAY['email']::text[], statement_timestamp())""",
            new_uuid(),
        )
        await conn.execute(
            f"""INSERT INTO {schema}.actor_config (actor, queue, max_concurrent)
                VALUES ('mailer', 'email', 2)"""
        )
        # The idle queue: due work, no worker, no terminalisations ever.
        await conn.execute(
            f"""INSERT INTO {schema}.jobs (
                    id, actor, queue, payload, max_attempts, retry_kind, status, scheduled_at
                ) VALUES (
                    $1, 'idle_actor', 'idle', '{{"v": 1}}'::jsonb, 3, 'transient',
                    $2::{schema}.job_status, statement_timestamp() - interval '10 minutes'
                )""",
            new_uuid(),
            "pending",
        )
        # One cron schedule with two fires: one terminalised, one pending
        # (the outstanding backlog).
        schedule_id = new_uuid()
        await conn.execute(
            f"""INSERT INTO {schema}.cron_schedules (id, actor, cron_expr, next_fire_at)
                VALUES ($1, 'ticker', '* * * * *', statement_timestamp() + interval '1 hour')""",
            schedule_id,
        )
        for status in ("succeeded", "pending"):
            await conn.execute(
                f"""INSERT INTO {schema}.jobs (
                        id, actor, queue, payload, max_attempts, retry_kind, status,
                        created_at, scheduled_at, started_at, finished_at, metadata
                    ) VALUES (
                        $1, 'ticker', 'email', '{{"v": 1}}'::jsonb, 3, 'transient',
                        $2::{schema}.job_status, $3, $3, $4, $4,
                        jsonb_build_object('cron_schedule_id', $5::text)
                    )""",
                new_uuid(),
                status,
                now - timedelta(minutes=10),
                now - timedelta(minutes=9),
                str(schedule_id),
            )
        env = {"TASKQ_PG_DSN": pg_dsn, "TASKQ_SCHEMA_NAME": schema}
        yield env
    finally:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


@pytest.mark.integration
def test_insights_all_against_real_pg(
    pg_env: dict[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """The full default surface against the seeded scenario: every section
    renders its seeded facts, the idle queue's drain is honest, and the
    command exits 0 (the read-only discipline)."""
    for key, value in pg_env.items():
        monkeypatch.setenv(key, value)

    result = runner.invoke(app, ["insights"])

    assert result.exit_code == 0, result.output
    plain = plain_cli_output(result.output)
    # wait: both segments render, labeled.
    assert "clean" in plain and "deferred" in plain
    # balance: the idle queue is the starvation shape, marked.
    assert "!!" in plain
    # drain: the idle queue carried no traffic — the honest string, never 0s.
    assert "no traffic in window" in plain
    # cron: the schedule's ledger rendered.
    assert "ticker" in plain


@pytest.mark.integration
def test_insights_drain_busy_queue_renders_an_eta_not_the_caveat(
    pg_env: dict[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    for key, value in pg_env.items():
        monkeypatch.setenv(key, value)

    result = runner.invoke(app, ["insights", "drain"])

    assert result.exit_code == 0, result.output
    plain = plain_cli_output(result.output)
    assert "email" in plain
    assert "no traffic in window" in plain, "the idle queue's row must carry the caveat"
    # The busy queue's row renders a humanized eta (never a bare 0s).
    email_line = next(
        (
            line
            for line in result.output.splitlines()
            if "email" in line and "no traffic" not in line
        ),
        "",
    )
    assert email_line, "the busy queue must render a drain row"
    # The eta CELL is never the bare "0s" (a zero would read as "already
    # drained"); "30m0s" legitimately contains the substring, so check the
    # cell, not the substring.
    assert email_line.split()[-1] != "0s"

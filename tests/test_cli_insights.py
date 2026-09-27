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

from collections.abc import AsyncIterator, Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from typer.testing import CliRunner

from taskq._ids import new_uuid
from taskq.cli import app
from taskq.migrate import apply_pending
from taskq.testing._shared_containers import creator_labels, skip_test_without_docker

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
    plain = " ".join(result.output.split())
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
    plain = " ".join(result.output.split()).lower()
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
    plain = " ".join(result.output.split())
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
    assert "nothing to report" in result.output


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
    plain = " ".join(result.output.split())
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
    plain = " ".join(result.output.split())
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
    assert "!!" not in result.output


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
    plain = " ".join(result.output.split())
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
    plain = " ".join(result.output.split())
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
    plain = " ".join(result.output.split())
    assert "4m0s" in plain
    assert "240.0" not in plain and "240s" not in plain


# ── cron ─────────────────────────────────────────────────────────────────


def test_cron_runaway_renders_the_trending_verdict(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_conn(monkeypatch, _FakeConn(cron_rows=[_cron_row(runaway_trending=True)]))

    result = runner.invoke(app, ["insights", "cron"])

    assert result.exit_code == 0
    plain = " ".join(result.output.split())
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
    assert "runaway" not in result.output


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
    plain = " ".join(result.output.split())
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
    plain = " ".join(result.output.split())
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
    assert "wait" in result.output, "the refusal must name the valid surfaces"


def test_invalid_window_exits_1_naming_the_closed_set(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = _FakeConn()
    _patch_conn(monkeypatch, conn)

    result = runner.invoke(app, ["insights", "wait", "--window", "30d"])

    assert result.exit_code == 1
    assert "1h" in result.output, "the refusal must name the valid windows"
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
    plain = " ".join(result.output.split())
    assert "email" in plain
    assert "batch" not in plain


def test_help_documents_every_column() -> None:
    """The command's own help is the operator's data dictionary: every
    rendered column's meaning (and the confounds) must be stated there."""
    result = runner.invoke(app, ["insights", "--help"])

    assert result.exit_code == 0
    plain = " ".join(result.output.split()).lower()
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
    plain = " ".join(result.output.split())
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
    plain = " ".join(result.output.split())
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

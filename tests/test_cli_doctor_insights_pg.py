# ruff: noqa: S608  # Why: schema is a fixture-derived test identifier, not user input; every value is $-bound.
"""`taskq doctor`'s operational-insight families against a REAL Postgres.

The four finding families are built on the already-merged
:mod:`taskq.insights` read layer (``fetch_queue_imbalance``,
``fetch_wait_distribution``'s p95 baseline, ``fetch_overprovisioning``,
``fetch_drain_estimates``, ``fetch_cron_ledger``).  The unit tier
(``test_cli_doctor.py``) fakes those reads at the ``taskq.cli`` boundary;
this tier seeds each family's PATHOLOGICAL shape and its HEALTHY control
in a real Postgres 18 container, runs the actual ``taskq doctor`` command
against the seeded schema, and pins the two things that make the report
usable: the pathological shape renders the finding WITH its actionable
remedy, and the healthy shape renders NOTHING (``no findings``).  A
finding that fires on health is noise an operator stops reading; the
no-finding pin is as important as the finding.

The read-only contract is re-proven on this tier against the REAL
statement set: every statement the command executes - the new insights
reads included - is captured by a recording connection proxy and scanned
for writing verbs.  A reporting tool that repairs what it finds destroys
the evidence of what went wrong.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg
import pytest
from pydantic import BaseModel
from typer.testing import CliRunner

from taskq._ids import new_job_id, new_uuid
from taskq.actor import ActorRef, actor
from taskq.cli import app
from taskq.migrate import apply_pending
from taskq.testing._shared_containers import skip_test_without_docker
from taskq.testing.assertions import plain_cli_output

pytestmark = pytest.mark.integration

runner = CliRunner()

# Writing SQL verbs: any of these appearing as a WORD in a statement the
# command issued means `doctor` is no longer read-only.  Word-boundary
# matched: a read selecting `updated_at` is not an UPDATE.
_WRITE_VERBS = re.compile(r"\b(insert|update|delete|truncate|drop|alter|create)\b", re.IGNORECASE)


class _Payload(BaseModel):
    value: int


@actor(name="pg_doctor_starved", queue="q_starved")
async def _starved(payload: _Payload) -> None: ...


@actor(name="pg_doctor_strand", queue="q_strand")
async def _strand(payload: _Payload) -> None: ...


@actor(name="pg_doctor_over", queue="q_over")
async def _over(payload: _Payload) -> None: ...


@actor(name="pg_doctor_drain", queue="q_drain")
async def _drain(payload: _Payload) -> None: ...


@actor(name="pg_doctor_cron", queue="q_cron")
async def _cron(payload: _Payload) -> None: ...


@actor(name="pg_doctor_healthy", queue="q_healthy")
async def _healthy(payload: _Payload) -> None: ...


_REGISTRY: dict[str, ActorRef[Any, Any]] = {
    "pg_doctor_starved": _starved,
    "pg_doctor_strand": _strand,
    "pg_doctor_over": _over,
    "pg_doctor_drain": _drain,
    "pg_doctor_cron": _cron,
    "pg_doctor_healthy": _healthy,
}
_REGISTRY_PATH = "tests.test_cli_doctor_insights_pg:_REGISTRY"


# ── Fixtures: one migrated schema per test, dropped on teardown ────────


@pytest.fixture
async def doctor_env(
    pg_dsn: str, request: pytest.FixtureRequest
) -> AsyncIterator[tuple[asyncpg.Connection, str, str]]:
    """A fresh migrated schema per test (unique name per node), yielded as
    ``(conn, schema, dsn)``.  Every test seeds its own shapes into an
    empty schema, so the healthy control is not polluted by a
    pathological sibling's rows."""
    skip_test_without_docker()
    schema = (
        "doctor_"
        + "".join(ch if ch.isalnum() else "_" for ch in request.node.name.lower())[:40]
        + "_"
        # Not a TaskQ id - a random schema-name suffix; UUIDv7's
        # time-ordered prefix would leak nothing but its length here.
        + uuid.uuid4().hex[:8]  # noqa: TID251
    )
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema)
        yield conn, schema, pg_dsn
    finally:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


async def _run_doctor(monkeypatch: pytest.MonkeyPatch, pg_dsn: str, schema: str) -> tuple[int, str]:
    """Run the real CLI against the seeded schema; return (exit code, output)."""
    # The env-var scan reads the REAL process environment; the developer's
    # ambient TASKQ_* variables must not decide what the report shows
    # (the same discipline test_cli_doctor._clean_taskq_env applies).
    for name in list(os.environ):
        if name.startswith("TASKQ_"):
            monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("TASKQ_PG_DSN", pg_dsn)
    monkeypatch.setenv("TASKQ_SCHEMA_NAME", schema)
    # The command's own entry point calls asyncio.run(); the test already
    # runs inside the module loop, so the invocation moves to a worker
    # thread where a private loop is legal — the same code path, the same
    # CliRunner.
    result = await asyncio.to_thread(runner.invoke, app, ["doctor", "--actors", _REGISTRY_PATH])
    if result.exit_code != 0:
        # Surface the real failure in the assertion message instead of a
        # bare exit code: the exception (if any) and the full report.
        detail = repr(result.exception) if result.exception else "(no exception)"
        return result.exit_code, f"{result.output}\n[{detail}]"
    # CI's runner env force-colorizes typer's rich-rendered surfaces (the
    # run-36369327985 class); the report's markers must be matched against
    # the plain bytes, never the colored ones.
    return result.exit_code, plain_cli_output(result.output)


# ── Seeding helpers (the test_insights.py shapes, trimmed to doctor's) ──


async def _seed_config(
    conn: asyncpg.Connection, schema: str, *, actor: str, queue: str, max_concurrent: int
) -> None:
    await conn.execute(
        f"""INSERT INTO {schema}.actor_config (actor, max_concurrent, max_pending, queue)
            VALUES ($1, $2, NULL, $3)""",
        actor,
        max_concurrent,
        queue,
    )


async def _seed_worker(conn: asyncpg.Connection, schema: str, *, queue: str) -> None:
    now = datetime.now(UTC)
    await conn.execute(
        f"""INSERT INTO {schema}.workers (id, hostname, pid, queues, started_at, last_seen_at)
            VALUES ($1, 'doctor-pg-host', 4242, $2, $3, $4)""",
        new_uuid(),
        [queue],
        now - timedelta(hours=1),
        now - timedelta(seconds=2),
    )


async def _seed_pending(
    conn: asyncpg.Connection,
    schema: str,
    *,
    queue: str,
    actor: str,
    scheduled_age_s: float,
    count: int = 1,
    metadata: dict[str, Any] | None = None,
    created_age_s: float | None = None,
) -> None:
    """Pending (due-now) rows; ``scheduled_age_s`` past means DUE now."""
    now = datetime.now(UTC)
    scheduled_at = now - timedelta(seconds=scheduled_age_s)
    created_at = scheduled_at if created_age_s is None else now - timedelta(seconds=created_age_s)
    meta = json.dumps(metadata or {})
    for _ in range(count):
        await conn.execute(
            f"""INSERT INTO {schema}.jobs (
                    id, actor, queue, payload, max_attempts, retry_kind, status,
                    created_at, scheduled_at, metadata
                ) VALUES ($1, $2, $3, '{{"v": 1}}'::jsonb, 3, 'transient',
                          'pending'::{schema}.job_status, $4, $5, $6::jsonb)""",
            new_job_id(),
            actor,
            queue,
            created_at,
            scheduled_at,
            meta,
        )


async def _seed_terminal(
    conn: asyncpg.Connection,
    schema: str,
    *,
    queue: str,
    actor: str,
    count: int,
    wait_s: float = 5.0,
    finished_age_s: float = 60.0,
    metadata: dict[str, Any] | None = None,
    created_age_s: float | None = None,
) -> None:
    """Succeeded rows with a ~*wait_s* wait (the p95 baseline) finished
    *finished_age_s* ago (inside the 24h insights window)."""
    now = datetime.now(UTC)
    created_at = now - timedelta(seconds=finished_age_s + wait_s + 60)
    if created_age_s is not None:
        created_at = now - timedelta(seconds=created_age_s)
    meta = json.dumps(metadata or {})
    for i in range(count):
        finished = now - timedelta(seconds=finished_age_s + i)
        started = finished - timedelta(milliseconds=50)
        scheduled = started - timedelta(seconds=wait_s)
        await conn.execute(
            f"""INSERT INTO {schema}.jobs (
                    id, actor, queue, payload, max_attempts, retry_kind, status,
                    created_at, scheduled_at, started_at, finished_at, metadata
                ) VALUES ($1, $2, $3, '{{"v": 1}}'::jsonb, 3, 'transient',
                          'succeeded'::{schema}.job_status, $4, $5, $6, $7, $8::jsonb)""",
            new_job_id(),
            actor,
            queue,
            created_at,
            scheduled,
            started,
            finished,
            meta,
        )


async def _seed_running(
    conn: asyncpg.Connection,
    schema: str,
    *,
    queue: str,
    actor: str,
    created_age_s: float,
    metadata: dict[str, Any] | None = None,
) -> None:
    """A RUNNING row (the in-flight fire the first-fire pin needs)."""
    now = datetime.now(UTC)
    await conn.execute(
        f"""INSERT INTO {schema}.jobs (
                id, actor, queue, payload, max_attempts, retry_kind, status,
                created_at, scheduled_at, started_at, metadata
            ) VALUES ($1, $2, $3, '{{"v": 1}}'::jsonb, 3, 'transient',
                      'running'::{schema}.job_status, $4, $5, $6, $7::jsonb)""",
        new_job_id(),
        actor,
        queue,
        now - timedelta(seconds=created_age_s),
        now - timedelta(seconds=created_age_s),
        now - timedelta(seconds=created_age_s / 2),
        json.dumps(metadata or {}),
    )


async def _seed_schedule(conn: asyncpg.Connection, schema: str, *, actor: str) -> uuid.UUID:
    sid = new_uuid()
    await conn.execute(
        f"""INSERT INTO {schema}.cron_schedules
                (id, actor, cron_expr, timezone, dst_strategy, next_fire_at)
            VALUES ($1, $2, '*/5 * * * *', 'UTC', 'skip', statement_timestamp())""",
        sid,
        actor,
    )
    return sid


# ── The pathological seed: one queue per family ─────────────────────────


async def _seed_pathological(conn: asyncpg.Connection, schema: str) -> uuid.UUID:
    """Every family's pathological shape at once, on disjoint queues so
    the findings are independent:

    * ``q_starved`` — 12 due jobs, effective capacity 1 (cap 1 x 1
      worker): utilization 12x, past the 2x threshold. The due rows'
      age sits past the 60s persistence floor (a depth younger than the
      claim-in-flight window is a burst, not an arrival-rate claim).
    * ``q_strand`` — one due job ~600s old against a p95 wait of 5s
      (the strand test's threshold: max(4 x p95, 60s) = 60s).
    * ``q_over`` — a live worker, zero due depth, zero terminalisations
      in the window: fewer than one completion per worker.
    * ``q_drain`` — 200 due jobs against 50 completions in the window:
      eta 4 days, beyond the 24h window the rate was measured over. The
      depth's age sits past the persistence floor (an idle-capacity
      fleet's young burst reads a fictional eta off its demand-limited
      rate).
    * a cron schedule — fires outran clearances in BOTH windows with a
      16-fire outstanding backlog (runaway trending AND above the
      catch-up window's demonstrated capacity).
    """
    await _seed_config(conn, schema, actor="pg_doctor_starved", queue="q_starved", max_concurrent=1)
    await _seed_config(conn, schema, actor="pg_doctor_strand", queue="q_strand", max_concurrent=50)
    await _seed_config(conn, schema, actor="pg_doctor_over", queue="q_over", max_concurrent=4)
    await _seed_config(conn, schema, actor="pg_doctor_drain", queue="q_drain", max_concurrent=1000)
    await _seed_config(conn, schema, actor="pg_doctor_cron", queue="q_cron", max_concurrent=100)
    for q in ("q_starved", "q_strand", "q_over", "q_drain", "q_cron"):
        await _seed_worker(conn, schema, queue=q)

    # Starved: 12 due rows against capacity 1, oldest row past the floor.
    await _seed_pending(
        conn, schema, queue="q_starved", actor="pg_doctor_starved", scheduled_age_s=90, count=12
    )
    # Strand: one 600s-old due row; the queue's p95 wait is 5s.
    await _seed_pending(
        conn, schema, queue="q_strand", actor="pg_doctor_strand", scheduled_age_s=600
    )
    await _seed_terminal(conn, schema, queue="q_strand", actor="pg_doctor_strand", count=20)
    # Drain: 200 due rows against 50 completions in the window, the depth
    # persisted past the persistence floor (a young burst's eta is fiction).
    await _seed_pending(
        conn, schema, queue="q_drain", actor="pg_doctor_drain", scheduled_age_s=90, count=200
    )
    await _seed_terminal(conn, schema, queue="q_drain", actor="pg_doctor_drain", count=50)

    # Cron runaway, stamped with the schedule's provenance metadata:
    #   current window — 10 fires (2 cleared, 8 outstanding)
    #   prior window   — 10 fires (2 cleared, 8 outstanding)
    sid = await _seed_schedule(conn, schema, actor="pg_doctor_cron")
    stamp = {"cron_schedule_id": str(sid)}
    await _seed_terminal(
        conn,
        schema,
        queue="q_cron",
        actor="pg_doctor_cron",
        count=2,
        metadata=stamp,
        created_age_s=600,
    )
    await _seed_terminal(
        conn,
        schema,
        queue="q_cron",
        actor="pg_doctor_cron",
        count=2,
        metadata=stamp,
        created_age_s=25 * 3600,
    )
    await _seed_pending(
        conn,
        schema,
        queue="q_cron",
        actor="pg_doctor_cron",
        scheduled_age_s=10,
        count=8,
        metadata=stamp,
        created_age_s=600,
    )
    await _seed_pending(
        conn,
        schema,
        queue="q_cron",
        actor="pg_doctor_cron",
        scheduled_age_s=10,
        count=8,
        metadata=stamp,
        created_age_s=25 * 3600,
    )
    return sid


async def test_doctor_renders_every_insight_finding_for_the_pathological_fleet(
    monkeypatch: pytest.MonkeyPatch,
    doctor_env: tuple[asyncpg.Connection, str, str],
) -> None:
    """The four pathological shapes, seeded in a real container, each
    render their finding WITH the actionable remedy; the command still
    exits 0 (a diagnostic that fails the shell gets wrapped in
    ``|| true`` and ignored)."""
    conn, schema, dsn = doctor_env
    sid = await _seed_pathological(conn, schema)

    exit_code, output = await _run_doctor(monkeypatch, dsn, schema)

    assert exit_code == 0, output

    # IMBALANCE — starved, with the real capacity levers.
    assert "q_starved" in output
    assert "STARVED" in output
    assert "max_concurrent" in output
    # IMBALANCE — strand, against the queue's own p95.
    assert "q_strand" in output
    assert "STRANDED WORK" in output
    # OVERPROVISIONING — consolidation, never destruction.
    assert "q_over" in output
    assert "OVERPROVISIONED" in output
    assert "consolidate" in output.lower()
    assert "workgroup" in output.lower()
    assert "nothing is deleted" in output.lower()
    assert "purge" not in output.lower()
    assert "drop" not in output.lower()
    # DRAIN — the eta and its confidence caveat.
    assert "q_drain" in output
    assert "SLOW DRAIN" in output
    assert "4.0 days" in output
    assert "extrapolation" in output.lower()
    # CRON LAG — the schedule, the backlog, both honest remedies.
    assert str(sid) in output
    assert "CRON LAG" in output
    assert "16 fire(s) outstanding" in output
    assert "slow the cron" in output.lower()
    assert "max_concurrent" in output


async def test_doctor_renders_nothing_for_the_healthy_fleet(
    monkeypatch: pytest.MonkeyPatch,
    doctor_env: tuple[asyncpg.Connection, str, str],
) -> None:
    """Control: a queue earning its worker (real throughput, no depth), a
    schedule clearing every fire in both windows - the report prints
    ``no findings`` and none of the family phrases.  Without this pin a
    command that flagged every queue would pass the finding tests above."""
    conn, schema, dsn = doctor_env
    # Every registered actor gets its stored row (a worker startup seeds
    # one) — the healthy deployment's premise.
    await _seed_config(conn, schema, actor="pg_doctor_healthy", queue="q_healthy", max_concurrent=4)
    for name, ref in _REGISTRY.items():
        if name != "pg_doctor_healthy":
            await _seed_config(conn, schema, actor=name, queue=ref.queue, max_concurrent=4)
    await _seed_worker(conn, schema, queue="q_healthy")
    await _seed_terminal(conn, schema, queue="q_healthy", actor="pg_doctor_healthy", count=10)

    # A fully-caught-up schedule: 5 fires in the current window and 5 in
    # the prior, every one cleared, nothing outstanding.
    sid = await _seed_schedule(conn, schema, actor="pg_doctor_healthy")
    stamp = {"cron_schedule_id": str(sid)}
    await _seed_terminal(
        conn,
        schema,
        queue="q_healthy",
        actor="pg_doctor_healthy",
        count=5,
        metadata=stamp,
        created_age_s=600,
    )
    await _seed_terminal(
        conn,
        schema,
        queue="q_healthy",
        actor="pg_doctor_healthy",
        count=5,
        metadata=stamp,
        created_age_s=25 * 3600,
    )

    exit_code, output = await _run_doctor(monkeypatch, dsn, schema)

    assert exit_code == 0, output
    # The storage-mode family (main's #574) is an informational line in
    # every report — the healthy fleet's ONLY finding. Any other family
    # phrase firing here is a false positive an operator stops reading.
    assert "findings (1):" in output, output
    assert "storage mode: " in output
    for phrase in ("STARVED", "STRANDED WORK", "OVERPROVISIONED", "SLOW DRAIN", "CRON LAG"):
        assert phrase not in output


async def test_doctor_is_silent_on_bursts_younger_than_the_persistence_floor(
    monkeypatch: pytest.MonkeyPatch,
    doctor_env: tuple[asyncpg.Connection, str, str],
) -> None:
    """The boundary's healthy side for the two DEPTH-derived arms: a
    healthy queue's 9-job burst (utilization 2.25x against capacity 4,
    sub-second service history) and an idle-capacity fleet's 51-job burst
    (a demand-limited rate reading a ~1.0-day eta) are both YOUNGER than
    the 60s persistence floor - the dispatcher absorbs them before the
    report is read, so neither may fire.  Measured before the floor
    existed: both fired on exactly this fleet."""
    conn, schema, dsn = doctor_env
    await _seed_config(conn, schema, actor="pg_doctor_starved", queue="q_starved", max_concurrent=4)
    await _seed_config(conn, schema, actor="pg_doctor_drain", queue="q_drain", max_concurrent=32)
    await _seed_worker(conn, schema, queue="q_starved")
    await _seed_worker(conn, schema, queue="q_drain")
    # healthy service histories
    await _seed_terminal(
        conn,
        schema,
        queue="q_starved",
        actor="pg_doctor_starved",
        count=40,
        wait_s=0.3,
        finished_age_s=1200,
    )
    await _seed_terminal(
        conn,
        schema,
        queue="q_drain",
        actor="pg_doctor_drain",
        count=50,
        wait_s=0.2,
        finished_age_s=3600,
    )
    # the bursts, seconds old
    await _seed_pending(
        conn, schema, queue="q_starved", actor="pg_doctor_starved", scheduled_age_s=2, count=9
    )
    await _seed_pending(
        conn, schema, queue="q_drain", actor="pg_doctor_drain", scheduled_age_s=2, count=51
    )

    exit_code, output = await _run_doctor(monkeypatch, dsn, schema)

    assert exit_code == 0, output
    assert "STARVED" not in output, output
    assert "SLOW DRAIN" not in output, output


async def test_doctor_is_silent_on_a_first_in_flight_cron_fire(
    monkeypatch: pytest.MonkeyPatch,
    doctor_env: tuple[asyncpg.Connection, str, str],
) -> None:
    """The right-edge guard's own case: a brand-new schedule whose FIRST
    fire is running right now.  cleared_window = cleared_prior = 0, so
    the demonstrated clearance is zero - a zero capacity would read every
    in-flight fire as an uncatchable backlog.  The arm requires a
    POSITIVE demonstrated clearance; the in-flight fire is work in
    flight, not a lag."""
    conn, schema, dsn = doctor_env
    await _seed_config(conn, schema, actor="pg_doctor_cron", queue="q_cron", max_concurrent=4)
    await _seed_worker(conn, schema, queue="q_cron")
    sid = await _seed_schedule(conn, schema, actor="pg_doctor_cron")
    await _seed_running(
        conn,
        schema,
        queue="q_cron",
        actor="pg_doctor_cron",
        created_age_s=120,
        metadata={"cron_schedule_id": str(sid)},
    )

    exit_code, output = await _run_doctor(monkeypatch, dsn, schema)

    assert exit_code == 0, output
    assert "CRON LAG" not in output, output


class _RecordingConn:
    """Proxy that records every statement and forwards to the real connection."""

    def __init__(self, inner: asyncpg.Connection) -> None:
        self._inner = inner
        self.statements: list[str] = []

    async def fetch(self, query: str, *args: Any) -> list[Any]:
        self.statements.append(query)
        return await self._inner.fetch(query, *args)

    async def fetchrow(self, query: str, *args: Any) -> Any:
        self.statements.append(query)
        return await self._inner.fetchrow(query, *args)

    async def fetchval(self, query: str, *args: Any) -> Any:
        self.statements.append(query)
        return await self._inner.fetchval(query, *args)

    async def execute(self, query: str, *args: Any) -> Any:
        self.statements.append(query)
        return await self._inner.execute(query, *args)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


async def test_doctor_issues_only_reads_against_the_real_statement_set(
    monkeypatch: pytest.MonkeyPatch,
    doctor_env: tuple[asyncpg.Connection, str, str],
) -> None:
    """The read-only contract, proven against the REAL statements the
    command issues - the insights reads included.  A recording proxy wraps
    the actual connection, so nothing is faked: every statement that runs
    is captured and scanned for writing verbs."""
    conn, schema, dsn = doctor_env
    await _seed_pathological(conn, schema)

    real_connect = asyncpg.connect
    recorders: list[_RecordingConn] = []

    async def recording_connect(dsn: str) -> Any:
        inner = await real_connect(dsn)
        recorder = _RecordingConn(inner)
        recorders.append(recorder)
        return recorder

    monkeypatch.setattr("taskq.cli.asyncpg.connect", recording_connect)

    exit_code, output = await _run_doctor(monkeypatch, dsn, schema)

    assert exit_code == 0, output
    executed = [statement for r in recorders for statement in r.statements]
    assert executed, "the command must have issued statements for this proof to mean anything"
    offenders = [s for s in executed if _WRITE_VERBS.search(s)]
    assert offenders == [], f"doctor issued writing statements: {offenders}"
    # The insights statements really were among the captured reads (the
    # imbalance statement names the CTE arm only it computes).
    assert any("effective_capacity" in statement for statement in executed)

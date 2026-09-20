# ruff: noqa: S608  # Why: schema is a fixed test identifier, not user input; every value is $-bound.
"""ATTACK tests around the prune ghost pins (branch fix/prune-archive-ghost-row).

The deterministic injection pins (test_leader_prune.py) fix ONE
interleaving each. These attacks hunt AROUND them:

* Gather-shaped races - many trials of real concurrent retries,
  re-terminalizations (old-stamp, zero-age, and half-age shapes) and a
  second concurrent prune, across batch sizes and statement timeouts.
  The master invariant is asserted on every trial: every seeded job is
  ALWAYS exactly one of live (jobs) or archived (jobs_archive), an
  archived row is terminal with a truthful finished_at, and its
  archived_at - finished_at age is at least the retention the write
  statement's lock-time re-check claims to verify (a zero-age archive
  is the lie the retention re-check exists to prevent).
* Races at the points the deterministic pins do not occupy: a retry
  committed before the candidate snapshot, and a retry issued the
  moment the write statement returns (its commit lands after the
  batch's commit - the archive-then-retry boundary).
* Archive column truth: jobs archived with varying claim counts
  (attempt) and full field population must carry the EXACT terminal
  version into jobs_archive (claim-epoch columns are PR 369's
  territory; they do not exist on this branch).
* Bounded writes through public surfaces: every write statement's id
  array is bounded by the batch size, an empty window runs no write,
  and validation refuses non-positive bounds.
"""

from __future__ import annotations

import asyncio
import contextlib
import random
import uuid
from datetime import UTC, datetime, timedelta

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._sql_templates import render
from taskq.backend.statemachine import TERMINAL_STATUSES
from taskq.settings import TaskQSettings
from taskq.worker.leader import prune_terminal_jobs

from .test_leader_prune import (
    _JOBS_COLUMNS,
    _apply,
    _seed_job_attempt,
    _seed_terminal_job,
    _seed_terminal_jobs_bulk,
)

pytestmark = pytest.mark.integration

_RETENTION = timedelta(days=30)
_ARCHIVE_RETENTION = timedelta(days=365)
_OLD = datetime.now(UTC) - timedelta(days=31)


async def _clean_slate(pg_conn: asyncpg.Connection, schema: str) -> None:
    """The pg schema persists across the module's tests: start every
    attack from empty tables."""
    await pg_conn.execute(
        f"TRUNCATE {schema}.jobs, {schema}.jobs_archive, "
        f"{schema}.job_attempts, {schema}.job_attempts_archive, {schema}.job_events CASCADE"
    )


# ── The master invariant ─────────────────────────────────────────────────


async def assert_master_invariant(
    conn: asyncpg.Connection,
    schema: str,
    *,
    retention: timedelta = _RETENTION,
    seeded: set[uuid.UUID] | None = None,
) -> None:
    """Every job is ALWAYS exactly one of live or archived; an archived
    row is terminal with a truthful terminal version and an age at least
    the retention the lock-time re-check verifies; no seeded job is lost."""
    live = await conn.fetch(f"SELECT * FROM {schema}.jobs")
    archived = await conn.fetch(f"SELECT * FROM {schema}.jobs_archive")
    live_ids = {r["id"] for r in live}
    arch_ids = {r["id"] for r in archived}
    ghosts = live_ids & arch_ids
    assert not ghosts, f"ghost rows (live AND archived): {ghosts}"
    for r in archived:
        assert r["status"] in TERMINAL_STATUSES, (
            f"archived row {r['id']} carries non-terminal status {r['status']}"
        )
        assert r["finished_at"] is not None, (
            f"archived row {r['id']} has no finished_at: not a terminal version"
        )
        age = r["archived_at"] - r["finished_at"]
        assert age >= retention, (
            f"archived row {r['id']} was archived at age {age}: the write "
            f"statement's lock-time retention re-check lied"
        )
    if seeded is not None:
        lost = seeded - live_ids - arch_ids
        assert not lost, f"jobs vanished from both tables: {lost}"


# ── Gather-shaped races ──────────────────────────────────────────────────


_RACER_SHAPES = ("retry_only", "reterm_old", "reterm_zero_age", "reterm_half_age")


async def _racer(
    conn: asyncpg.Connection,
    schema: str,
    ids: list[uuid.UUID],
    stop: asyncio.Event,
    rng: random.Random,
) -> int:
    """Hammer retries and re-terminalizations at the window's rows until
    the prune settles. Returns the number of statements it landed."""
    retry_sql = render(schema).retry_job
    reterm_old = (
        f"UPDATE {schema}.jobs SET status = $1::{schema}.job_status, finished_at = $2 WHERE id = $3"
    )
    reterm_zero_age = (
        f"UPDATE {schema}.jobs SET status = $1::{schema}.job_status, "
        f"finished_at = clock_timestamp() WHERE id = $2"
    )
    landed = 0
    while not stop.is_set():
        jid = rng.choice(ids)
        shape = rng.choice(_RACER_SHAPES)
        try:
            if shape == "retry_only":
                row = await conn.fetchrow(retry_sql, jid)
                if row is not None:
                    landed += 1
            elif shape == "reterm_old":
                # Retry then re-terminalize at the OLD stamp: the row
                # stays a valid, genuinely-aged candidate.
                await conn.execute(reterm_old, "succeeded", _OLD, jid)
                landed += 2
            elif shape == "reterm_zero_age":
                # Retry then re-fail NOW: terminal again but zero seconds
                # old - the shape the age re-check must keep out.
                await conn.fetchrow(retry_sql, jid)
                await conn.execute(reterm_zero_age, "succeeded", jid)
                landed += 2
            else:  # reterm_half_age
                # Half-aged: past nothing, must never archive.
                await conn.execute(
                    reterm_old, "failed", datetime.now(UTC) - timedelta(days=15), jid
                )
                landed += 1
        except asyncpg.exceptions.ObjectNotInPrerequisiteStateError:
            continue  # the row left the table mid-race; pick another
    return landed


async def _one_gather_trial(
    pg_conn: asyncpg.Connection,
    settings: TaskQSettings,
    ids: list[uuid.UUID],
    *,
    batch_size: int,
    timeout_ms: int,
    second_prune: bool,
    rng: random.Random,
) -> dict[str, object]:
    schema = settings.schema_name
    racer_conns = [await asyncpg.connect(str(settings.pg_dsn)) for _ in range(2)]
    second_conn = await asyncpg.connect(str(settings.pg_dsn))
    for c in [*racer_conns, second_conn]:
        await c.execute("SET lock_timeout = '10s'")
    stop = asyncio.Event()
    try:
        prune_task = asyncio.create_task(
            prune_terminal_jobs(
                pg_conn,
                retention_per_status={"succeeded": _RETENTION},
                archive_retention=_ARCHIVE_RETENTION,
                batch_size=batch_size,
                schema=schema,
                statement_timeout_ms=timeout_ms,
            )
        )
        second_task = (
            asyncio.create_task(
                prune_terminal_jobs(
                    second_conn,
                    retention_per_status={"succeeded": _RETENTION},
                    archive_retention=_ARCHIVE_RETENTION,
                    batch_size=batch_size,
                    schema=schema,
                    statement_timeout_ms=timeout_ms,
                )
            )
            if second_prune
            else None
        )
        racers = [asyncio.create_task(_racer(c, schema, ids, stop, rng)) for c in racer_conns]
        prune_error: BaseException | None = None
        try:
            await prune_task
        except Exception as exc:  # Why: a timeout-aborted batch is an allowed outcome; the invariant check below is not.
            prune_error = exc
        if second_task is not None:
            with contextlib.suppress(Exception):
                await second_task
        stop.set()
        racer_counts = await asyncio.gather(*racers, return_exceptions=True)

        await assert_master_invariant(pg_conn, schema, seeded=set(ids))
        return {
            "prune_error": type(prune_error).__name__ if prune_error else None,
            "racer_statements": sum(c for c in racer_counts if isinstance(c, int)),
            "racer_errors": [
                type(c).__name__ for c in racer_counts if isinstance(c, BaseException)
            ],
        }
    finally:
        stop.set()
        for c in [*racer_conns, second_conn]:
            await c.close()


@pytest.mark.parametrize("batch_size,timeout_ms", [(1, 4000), (2, 4000), (7, 1000), (50, 4000)])
@pytest.mark.parametrize("trial", range(5))
async def test_gather_race_never_loses_a_job(
    pg_conn: asyncpg.Connection,
    settings: TaskQSettings,
    batch_size: int,
    timeout_ms: int,
    trial: int,
) -> None:
    """Gather-shaped race trials (2 racers x 4 shapes, plus a second
    concurrent prune in one of three trials): across batch sizes and
    statement timeouts, no trial may lose a job, ghost a row, or archive
    at zero age."""
    await _apply(pg_conn, settings)
    schema = settings.schema_name
    await _clean_slate(pg_conn, schema)
    rng = random.Random(1000 * batch_size + trial)  # noqa: S311  # Why: reproducible race schedules, not cryptography.
    n_jobs = 24
    ids = await _seed_terminal_jobs_bulk(
        pg_conn, schema, count=n_jobs, status="succeeded", finished_at=_OLD
    )
    outcome = await _one_gather_trial(
        pg_conn,
        settings,
        ids,
        batch_size=batch_size,
        timeout_ms=timeout_ms,
        second_prune=trial % 2 == 1,
        rng=rng,
    )
    # The trial actually raced: the racers landed statements against the
    # window's rows.
    assert outcome["racer_statements"] > 0, f"the racers never landed: {outcome}"  # type: ignore[operator]  # Why: int by construction in _one_gather_trial.
    live_ids = {r["id"] for r in await pg_conn.fetch(f"SELECT id FROM {schema}.jobs")}
    arch_ids = {r["id"] for r in await pg_conn.fetch(f"SELECT id FROM {schema}.jobs_archive")}
    # Every seeded job resolved to exactly one side (assert_master_invariant
    # already proved no overlaps and no losses); report the partition.
    assert len(live_ids) + len(arch_ids) == n_jobs


# ── Races at the points the deterministic pins do not occupy ─────────────


async def test_retry_committed_before_the_candidate_snapshot_is_untouched(
    pg_conn: asyncpg.Connection, settings: TaskQSettings
) -> None:
    """A retry that commits BEFORE the prune's candidate window makes the
    row live-pending: the window must not select it, nothing about the
    row may move."""
    await _apply(pg_conn, settings)
    schema = settings.schema_name
    await _clean_slate(pg_conn, schema)
    jid = await _seed_terminal_job(pg_conn, status="succeeded", finished_at=_OLD, schema=schema)
    retry_sql = render(schema).retry_job
    retried = await pg_conn.fetchrow(retry_sql, jid)
    assert retried is not None, "fixture broken: the retry did not match the seeded row"

    result = await prune_terminal_jobs(
        pg_conn,
        retention_per_status={"succeeded": _RETENTION},
        archive_retention=_ARCHIVE_RETENTION,
        schema=schema,
    )
    assert result.total_deleted == 0
    assert result.archived == 0
    live = await pg_conn.fetchrow(f"SELECT status FROM {schema}.jobs WHERE id = $1", jid)
    assert live is not None and live["status"] == "pending", (
        "the prune moved a row whose retry committed before its snapshot"
    )
    await assert_master_invariant(pg_conn, schema, seeded={jid})


class _RetryAtWriteReturnConn:
    """ConnLike proxy that fires a retry on a SECOND connection the
    moment the write statement returns - the retry's UPDATE blocks on
    the row locks the still-uncommitted batch holds, so its commit lands
    after the batch's commit: the archive-then-retry boundary."""

    def __init__(
        self,
        inner: asyncpg.Connection,
        race_conn: asyncpg.Connection,
        retry_sql: str,
        jid: uuid.UUID,
    ) -> None:
        self._inner = inner
        self._race_conn = race_conn
        self._retry_sql = retry_sql
        self._jid = jid
        self.write_ran = False
        self.retry_matched: int | None = None
        self._retry_task: asyncio.Task[asyncpg.Record | None] | None = None

    def transaction(self) -> object:
        return self._inner.transaction()

    async def fetchval(self, sql: str, *args: object) -> object:
        return await self._inner.fetchval(sql, *args)

    async def fetch(self, sql: str, *args: object) -> list[asyncpg.Record]:
        rows = await self._inner.fetch(sql, *args)
        if not self.write_ran and "jobs_archive" in sql and "INSERT INTO" in sql:
            self.write_ran = True
            # The batch holds the row locks; this retry parks until the
            # batch's commit releases them.
            self._retry_task = asyncio.create_task(
                self._race_conn.fetchrow(self._retry_sql, self._jid)
            )
            await asyncio.sleep(0)  # let the retry park on the locks
        return rows

    async def execute(self, sql: str, *args: object) -> str:
        return await self._inner.execute(sql, *args)

    async def finish(self) -> None:
        assert self._retry_task is not None, "the write statement never ran"
        row = await self._retry_task
        self.retry_matched = row is not None


async def test_retry_issued_at_write_return_commits_after_the_archive(
    pg_conn: asyncpg.Connection, settings: TaskQSettings
) -> None:
    """A retry issued the instant the write statement returns cannot
    resurrect the row or ghost it: the archive committed first, the
    retry's UPDATE matches nothing, and the archive keeps exactly one
    terminal row. The retry caller observes the miss (its hand-back did
    not land) - that is the archive-then-retry boundary, never a silent
    loss of a row the archive does not hold."""
    await _apply(pg_conn, settings)
    schema = settings.schema_name
    await _clean_slate(pg_conn, schema)
    jid = await _seed_terminal_job(pg_conn, status="succeeded", finished_at=_OLD, schema=schema)
    retry_sql = render(schema).retry_job

    race_conn = await asyncpg.connect(str(settings.pg_dsn))
    await race_conn.execute("SET lock_timeout = '10s'")
    proxy = _RetryAtWriteReturnConn(pg_conn, race_conn, retry_sql, jid)
    try:
        result = await prune_terminal_jobs(
            proxy,
            retention_per_status={"succeeded": _RETENTION},
            archive_retention=_ARCHIVE_RETENTION,
            schema=schema,
        )
        await proxy.finish()
    finally:
        await race_conn.close()

    assert proxy.write_ran, "the write statement never ran"
    assert result.total_deleted == 1
    assert proxy.retry_matched is False, (
        "the post-archive retry matched a row: the archive and the live row coexist - a ghost"
    )
    live = await pg_conn.fetchval(f"SELECT count(*) FROM {schema}.jobs WHERE id = $1", jid)
    assert live == 0
    await assert_master_invariant(pg_conn, schema, seeded={jid})


# ── Archive column truth ─────────────────────────────────────────────────


async def test_archive_carries_the_true_terminal_version(
    pg_conn: asyncpg.Connection, settings: TaskQSettings
) -> None:
    """Jobs archived with varying claim counts (attempt) and fully
    populated fields must surface in jobs_archive with EVERY mirrored
    column equal to the live row's last terminal version - the archive
    is the row's truthful history, not a reconstruction."""
    await _apply(pg_conn, settings)
    schema = settings.schema_name
    now = datetime.now(UTC)
    seeded: set[uuid.UUID] = set()
    attempts = [0, 1, 7, 30000]
    expected_rows: dict[uuid.UUID, asyncpg.Record] = {}
    expected_attempt_history: dict[uuid.UUID, int] = {}
    for i, attempt in enumerate(attempts):
        jid = await _seed_terminal_job(
            pg_conn,
            status="failed" if i % 2 == 0 else "succeeded",
            finished_at=_OLD,
            schema=schema,
            actor=f"actor_{i}",
        )
        seeded.add(jid)
        # Populate the claim-count and terminal-truth columns directly:
        # the job was claimed *attempt* times, ran, and failed/succeeded
        # with full error/result bookkeeping.
        await pg_conn.execute(
            f"""UPDATE {schema}.jobs SET
                attempt = $2, started_at = $3, last_heartbeat_at = $3,
                locked_by_worker = $4, lock_expires_at = $5,
                error_class = $6, error_message = $7, error_traceback = $8,
                progress_state = $9::jsonb, progress_seq = $10,
                result = $11::jsonb, result_size_bytes = $12,
                identity_key = $13, fairness_key = $14,
                idempotency_key = $15, trace_id = $16, span_id = $17,
                metadata = $18::jsonb, tags = $19
            WHERE id = $1""",
            jid,
            attempt,
            now - timedelta(hours=2),
            new_uuid(),  # the row's locked_by_worker: any worker id, time-ordered per repo discipline
            now + timedelta(hours=1),
            "ValueError" if i % 2 == 0 else None,
            "boom" if i % 2 == 0 else None,
            "traceback-text" if i % 2 == 0 else None,
            '{"step": 3}',
            i,
            '{"answer": 42}',
            11,
            f"identity-{i}",
            f"fair-{i}" if i % 2 == 0 else None,
            f"idem-{i}" if i % 3 == 0 else None,
            f"trace-{i}",
            f"span-{i}",
            '{"origin": "attack"}',
            [f"tag-{i}"],
        )
        expected_attempt_history[jid] = min(attempt, 3)
        for a in range(1, expected_attempt_history[jid] + 1):
            await _seed_job_attempt(pg_conn, jid, schema=schema, attempt=a)
        row = await pg_conn.fetchrow(f"SELECT * FROM {schema}.jobs WHERE id = $1", jid)
        assert row is not None
        expected_rows[jid] = row

    result = await prune_terminal_jobs(
        pg_conn,
        retention_per_status={"succeeded": _RETENTION, "failed": _RETENTION},
        archive_retention=_ARCHIVE_RETENTION,
        schema=schema,
    )
    assert result.total_deleted == len(attempts)
    await assert_master_invariant(pg_conn, schema, seeded=seeded)

    mirrored = list(_JOBS_COLUMNS)
    for jid, expected in expected_rows.items():
        archived = await pg_conn.fetchrow(f"SELECT * FROM {schema}.jobs_archive WHERE id = $1", jid)
        assert archived is not None, f"job {jid} never archived"
        for col in mirrored:
            assert archived[col] == expected[col], (
                f"job {jid}: archive column {col} reads {archived[col]!r}, "
                f"the live terminal row said {expected[col]!r}"
            )
        # The attempts moved with the job, claim counts included.
        live_attempts = await pg_conn.fetchval(
            f"SELECT count(*) FROM {schema}.job_attempts WHERE job_id = $1", jid
        )
        archived_attempts = await pg_conn.fetchval(
            f"SELECT count(*) FROM {schema}.job_attempts_archive WHERE job_id = $1", jid
        )
        assert live_attempts == 0, "the live attempts rows did not move with the job"
        assert archived_attempts == expected_attempt_history[jid], (
            f"job {jid}: the archive kept {archived_attempts} of "
            f"{expected_attempt_history[jid]} attempt-history rows"
        )


# ── Bounded writes through public surfaces ───────────────────────────────


class _WriteSetRecordingConn:
    """ConnLike proxy recording the id array every write statement binds -
    the write set the sweepaudit exemption bounds."""

    def __init__(self, inner: asyncpg.Connection) -> None:
        self._inner = inner
        self.write_sets: list[list[uuid.UUID]] = []
        self.candidate_sets: list[int] = []
        self.write_calls = 0
        self._last_candidate_count = 0

    def transaction(self) -> object:
        return self._inner.transaction()

    async def fetchval(self, sql: str, *args: object) -> object:
        return await self._inner.fetchval(sql, *args)

    async def fetch(self, sql: str, *args: object) -> list[asyncpg.Record]:
        rows = await self._inner.fetch(sql, *args)
        if "ORDER BY finished_at" in sql:
            self._last_candidate_count = len(rows)
        if "jobs_archive" in sql and "INSERT INTO" in sql:
            self.write_calls += 1
            ids_arg = args[2]
            assert isinstance(ids_arg, list), f"the write statement bound {type(ids_arg)}"
            self.write_sets.append(list(ids_arg))
            self.candidate_sets.append(self._last_candidate_count)
        return rows

    async def execute(self, sql: str, *args: object) -> str:
        return await self._inner.execute(sql, *args)


async def test_prune_write_set_is_bounded_by_the_batch_size(
    pg_conn: asyncpg.Connection, settings: TaskQSettings
) -> None:
    """Every archive write's id array stays within the batch size the
    public call was given, an empty window runs no write statement, a
    huge batch size drains everything in one bounded batch, and
    non-positive bounds are refused."""
    await _apply(pg_conn, settings)
    schema = settings.schema_name
    await _clean_slate(pg_conn, schema)
    ids = await _seed_terminal_jobs_bulk(
        pg_conn, schema, count=23, status="succeeded", finished_at=_OLD
    )

    proxy = _WriteSetRecordingConn(pg_conn)
    result = await prune_terminal_jobs(
        proxy,
        retention_per_status={"succeeded": _RETENTION},
        archive_retention=_ARCHIVE_RETENTION,
        batch_size=5,
        schema=schema,
    )
    assert result.total_deleted == 23
    assert proxy.write_calls > 1, "23 jobs at batch size 5 must take several batches"
    written: list[uuid.UUID] = []
    for write_set, candidate_count in zip(proxy.write_sets, proxy.candidate_sets, strict=True):
        assert len(write_set) <= 5, (
            f"a write statement bound {len(write_set)} ids past the batch size 5"
        )
        assert candidate_count <= 5, "the candidate window ran past its LIMIT"
        written.extend(write_set)
    assert sorted(written) == sorted(ids), "the batches' union must be exactly the window"
    assert len(set(written)) == len(written), "a row was written twice"
    await assert_master_invariant(pg_conn, schema, seeded=set(ids))

    # Empty window: nothing eligible, no write statement at all.
    empty_proxy = _WriteSetRecordingConn(pg_conn)
    empty = await prune_terminal_jobs(
        empty_proxy,
        retention_per_status={"succeeded": timedelta(days=3650)},
        archive_retention=_ARCHIVE_RETENTION,
        batch_size=5,
        schema=schema,
    )
    assert empty.total_deleted == 0
    assert empty_proxy.write_calls == 0, "an empty window must not run a write statement"

    # Huge batch size: everything in one batch, still exactly the window.
    big_proxy = _WriteSetRecordingConn(pg_conn)
    await _seed_terminal_jobs_bulk(pg_conn, schema, count=5, status="failed", finished_at=_OLD)
    big = await prune_terminal_jobs(
        big_proxy,
        retention_per_status={"failed": _RETENTION},
        archive_retention=_ARCHIVE_RETENTION,
        batch_size=10**9,
        schema=schema,
    )
    assert big.total_deleted == 5
    assert len(big_proxy.write_sets) == 1 and len(big_proxy.write_sets[0]) == 5
    await assert_master_invariant(pg_conn, schema)

    # Non-positive bounds are refused at the boundary.
    with pytest.raises(ValueError):
        await prune_terminal_jobs(
            pg_conn,
            retention_per_status={"succeeded": _RETENTION},
            archive_retention=_ARCHIVE_RETENTION,
            batch_size=0,
            schema=schema,
        )
    with pytest.raises(ValueError):
        await prune_terminal_jobs(
            pg_conn,
            retention_per_status={"succeeded": _RETENTION},
            archive_retention=_ARCHIVE_RETENTION,
            batch_size=-1,
            schema=schema,
        )

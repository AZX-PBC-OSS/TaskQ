# ruff: noqa: S608  # Why: schema is a fixed test identifier, not user input; every value is $-bound.
"""ATTACK tests: the prune/archive path integrated with optional TimescaleDB
hypertables and the row-level expiry sweeps (branch fix/rt-retention territory).

The deterministic pins (test_leader_prune.py, test_attack_prune_ghost.py,
test_index_audit.py) fix one interleaving each on vanilla Postgres. These
attacks hunt the INTEGRATION surface where the prune/archive machinery meets
the optional hypertable mode and the flag boundary:

* The FOLD path under both engines, looped: a stale ghost (an archive row
  whose job retried and re-terminalized) must converge to exactly one
  archive row on the first prune and NEVER re-enter the candidate window
  (probed directly, statement by statement, not just through counts).
* Re-archive racing the chunk boundary: the fold runs while a REAL
  retention-policy run drops the standing row's chunk underneath it.
* Expiry sweep vs drop_chunks on the same rows: no double-delete, no
  resurrection, and the final state is the union of the two mechanisms'
  legitimate targets.
* The two directions of the chunk-granularity vs expire_at gap: rows
  outliving expire_at inside a young chunk are bounded by one chunk
  interval (verified against the live catalogs), and a chunk holding
  write-stamped rows (expire_at = finished_at + archive_retention) can
  never drop before its rows expire (verified as a catalog invariant that
  follows from the write's stamping contract).
* Plan truth on the hypertable: the expiry sweep's bound is expire_at -
  NOT the partition column - so chunk startup-pruning is impossible for it
  by construction; the pin is per-chunk index-boundedness (no per-chunk
  population walk), while partition-column windows must prune chunks.
* The flag boundary on one database: off -> on -> off -> on converges with
  no duplicate policies, no orphaned chunks, the ledger checksums
  untouched, the off position a verified ZERO-SQL gate, and the vanilla
  runtime paths unharmed on a still-converted schema after a downgrade
  attempt (runtime code never branches on the mode).
* The loud refusal on a vanilla server leaves the migration ledger and the
  prune path untouched.
* The bounded-writes discipline enforced at the SQL itself: the write
  statement moves exactly the ids bound in its array - a subset candidate
  cannot leak past it and an empty array moves nothing.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._sql_templates import render
from taskq.migrate import apply_pending
from taskq.settings import TaskQSettings, WorkerSettings
from taskq.testing._shared_containers import creator_labels, skip_test_without_docker
from taskq.timescale import (
    HypertableReport,
    TimescaleDBUnavailableError,
    enable_hypertables,
)
from taskq.worker._leader_shared import (  # pyright: ignore[reportPrivateUsage]  # Why: the attack binds the exact statements the sweep machinery renders, so a drift in either fails here first.
    _ARCHIVE_CANDIDATE_SQL,
    _ARCHIVE_CTE_SQL,
    _EXPIRY_CTE_SQL,
)
from taskq.worker.leader import archive_expiry_sweep, prune_terminal_jobs

from .test_attack_prune_ghost import assert_master_invariant
from .test_leader_prune import _apply, _seed_terminal_job, _seed_terminal_jobs_bulk
from .test_timescaledb_hypertables import _RefusingConn, _schedule_policies

pytestmark = pytest.mark.integration

# The fold scenario's clocks. Prune retention 6h makes both ghost stamps
# (30h and 54h old) aged candidates; the two stamps are 30h apart, which
# 1-day chunks cannot span, so on the hypertable the ghost's two versions
# are GUARANTEED to sit in different chunks - the exact shape where a
# missing archive-once guard would silently duplicate (UNIQUE (id,
# finished_at) cannot catch a second row with a different finished_at).
_FOLD_RETENTION = timedelta(hours=6)
_FOLD_ARCHIVE_RETENTION = timedelta(days=2)

#: Headroom for the stamping contract's upper side. archived_at and
#: expire_at are two separate clock_timestamp() evaluations in one target
#: list - two independent server clock reads, not one - so the pair can
#: straddle a microsecond tick, and the DB host's scheduler can put a few
#: microseconds between the two evaluations (CI observed retention + 3us).
#: A LATER expire_at keeps the row longer - the safe direction - so the
#: data-safety side below pins tight while this side only guards the
#: contract against a real stamping bug (a wrong retention misses by
#: seconds/minutes, not by the scheduler's microseconds).
_STAMP_GAP_SANITY = timedelta(seconds=1)
_STANDING_FIN = timedelta(hours=30)
_LIVE_FIN = timedelta(hours=54)
_NOW = datetime.now(UTC)


# ── Shared seeding ───────────────────────────────────────────────────────


async def _seed_ghost_pair(
    conn: asyncpg.Connection,
    schema: str,
    *,
    standing_fin: datetime,
    live_fin: datetime,
) -> Any:
    """A stale ghost: one id with a STANDING archive row (the pre-retry
    version) and a LIVE terminal row (the retried, re-terminalized
    version). This is the state a pre-guard wedge used to die on."""
    gid = new_uuid()
    now = datetime.now(UTC)
    await conn.execute(
        f"""INSERT INTO {schema}.jobs_archive (
            id, actor, queue, payload, max_attempts, retry_kind, status,
            scheduled_at, schedule_to_close, finished_at, archived_at, expire_at
        ) VALUES ($1, 'test_actor', 'default', '{{"v":1}}'::jsonb, 3, 'transient',
            'succeeded', $2, $3, $4, $5, $6)""",
        gid,
        standing_fin,
        standing_fin + timedelta(hours=1),
        standing_fin,
        now,
        standing_fin + _FOLD_ARCHIVE_RETENTION,
    )
    await conn.execute(
        f"""INSERT INTO {schema}.jobs (
            id, actor, queue, payload, max_attempts, retry_kind, status,
            scheduled_at, schedule_to_close, finished_at
        ) VALUES ($1, 'test_actor', 'default', '{{"v":1}}'::jsonb, 3, 'transient',
            'succeeded', $2, $3, $4)""",
        gid,
        live_fin,
        live_fin + timedelta(hours=1),
        live_fin,
    )
    return gid


async def _ghost_facts(conn: asyncpg.Connection, schema: str, gid: Any) -> dict[str, int]:
    return {
        "live": await conn.fetchval(f"SELECT count(*) FROM {schema}.jobs WHERE id = $1", gid),
        "archived": await conn.fetchval(
            f"SELECT count(*) FROM {schema}.jobs_archive WHERE id = $1", gid
        ),
    }


async def _candidate_ids(
    conn: asyncpg.Connection, schema: str, *, status: str, retention: timedelta, limit: int = 100
) -> list[Any]:
    """Run the EXACT candidate window the prune runs and return its ids -
    the direct probe for 'no re-entry into the candidate window forever'."""
    rows = await conn.fetch(
        _ARCHIVE_CANDIDATE_SQL.format(schema=schema),
        status,
        retention,
        limit,
    )
    return [r["id"] for r in rows]


async def _migrate_ts(
    conn: asyncpg.Connection, schema: str, dsn: str, *, flag: bool
) -> WorkerSettings:
    """Migrate one fresh schema and (when the flag is on) convert it,
    deferring the retention policies so the background workers cannot fire
    mid-test (the legs that need a real policy run pull next_start back
    themselves)."""
    await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    await apply_pending(conn, schema=schema)
    settings = _ts_settings(dsn, schema, flag=flag)
    if flag:
        report = await enable_hypertables(conn, schema=schema, settings=settings)
        assert set(report.converted) == {
            "job_events",
            "jobs_archive",
            "job_attempts_archive",
        }
        await _schedule_policies(conn, schema, next_start=datetime.now(UTC) + timedelta(days=3650))
    return settings


def _ts_settings(dsn: str, schema: str, *, flag: bool) -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": dsn,
            "TASKQ_SCHEMA_NAME": schema,
            "TASKQ_TIMESCALEDB_HYPERTABLES": "true" if flag else "false",
            "TASKQ_ARCHIVE_RETENTION_PERIOD": f"{int(_FOLD_ARCHIVE_RETENTION.total_seconds())}s",
        }
    )


# ── 1. The fold path, looped, under BOTH engines ─────────────────────────


async def _fold_loop_scenario(dsn: str, schema: str, *, convert: bool) -> None:
    """Seed a stale ghost plus two ordinary aged jobs; run the prune once,
    then LOOP it, asserting convergence and permanent non-reentry."""
    conn = await asyncpg.connect(dsn)
    try:
        await _migrate_ts(conn, schema, dsn, flag=convert)
        # The vanilla leg shares the module schema with this file's other
        # vanilla legs: start every run from empty tables.
        await conn.execute(
            f"TRUNCATE {schema}.jobs, {schema}.jobs_archive, "
            f"{schema}.job_attempts, {schema}.job_attempts_archive, {schema}.job_events CASCADE"
        )
        ordinary = await _seed_terminal_jobs_bulk(
            conn, schema, count=2, status="succeeded", finished_at=_NOW - timedelta(days=31)
        )
        gid = await _seed_ghost_pair(
            conn,
            schema,
            standing_fin=_NOW - _STANDING_FIN,
            live_fin=_NOW - _LIVE_FIN,
        )

        first = await prune_terminal_jobs(
            conn,
            retention_per_status={"succeeded": _FOLD_RETENTION},
            archive_retention=_FOLD_ARCHIVE_RETENTION,
            schema=schema,
            batch_size=100,
        )
        # Both ordinary jobs archived AND the ghost's live row removed in
        # the same drain (the fold deletes via `verified`, not via `moved`).
        assert first.total_deleted == 3, f"first prune: {first.total_deleted}"
        facts = await _ghost_facts(conn, schema, gid)
        assert facts == {"live": 0, "archived": 1}, f"post-fold: {facts}"

        # NO RE-ENTRY, forever: the candidate window itself must stop
        # selecting the folded id, and five further drains must each be a
        # no-op with the archive row count pinned at exactly one.
        for round_no in range(5):
            assert gid not in await _candidate_ids(
                conn, schema, status="succeeded", retention=_FOLD_RETENTION
            ), f"round {round_no}: the folded ghost re-entered the candidate window"
            again = await prune_terminal_jobs(
                conn,
                retention_per_status={"succeeded": _FOLD_RETENTION},
                archive_retention=_FOLD_ARCHIVE_RETENTION,
                schema=schema,
                batch_size=100,
            )
            assert again.total_deleted == 0, f"round {round_no} re-archived: {again.total_deleted}"
            facts = await _ghost_facts(conn, schema, gid)
            assert facts == {"live": 0, "archived": 1}, f"round {round_no}: {facts}"

        # The two ordinary jobs remain exactly-once archived, and the
        # master invariant holds over the whole population.
        n_archive = await conn.fetchval(f"SELECT count(*) FROM {schema}.jobs_archive")
        assert n_archive == 3
        await assert_master_invariant(
            conn, schema, retention=_FOLD_RETENTION, seeded={gid, *ordinary}
        )
    finally:
        await conn.close()


async def test_vanilla_ghost_fold_converges_and_never_reenters(
    pg_dsn: str, module_pg_schema: Any
) -> None:
    """Vanilla mode: the fold is the archive-once guard + the verified CTE;
    the looped probe must show permanent convergence."""
    await _fold_loop_scenario(pg_dsn, module_pg_schema.schema_name, convert=False)


async def test_timescale_ghost_fold_converges_and_never_reenters(
    timescale_dsn: str, ts_schema: str
) -> None:
    """Hypertable mode: the ghost's two versions sit in DIFFERENT chunks
    (their stamps are 30h apart), so UNIQUE (id, finished_at) cannot catch
    a re-archive by accident - the NOT EXISTS guard is the only thing
    standing. The looped probe must show the fold, not a duplicate."""
    await _fold_loop_scenario(timescale_dsn, ts_schema, convert=True)


# ── 2. Deterministic attack on the lock-time age re-check ─────────────────


class _RetermBetweenWindowAndWriteConn:
    """ConnLike proxy that, the instant the candidate window returns (it
    holds NO row locks - that is its design), retries a seeded job and
    re-terminalizes it with a FRESH finished_at on a second connection.
    The write statement's lock-time re-check is what must then drop the
    zero-age row instead of archiving it as a ghost."""

    def __init__(
        self,
        inner: asyncpg.Connection,
        race_conn: asyncpg.Connection,
        schema: str,
        jid: Any,
    ) -> None:
        self._inner = inner
        self._race = race_conn
        self._schema = schema
        self._jid = jid
        self.fired = False

    def transaction(self) -> object:
        return self._inner.transaction()

    async def fetchval(self, sql: str, *args: object) -> object:
        return await self._inner.fetchval(sql, *args)

    async def fetch(self, sql: str, *args: object) -> list[asyncpg.Record]:
        rows = await self._inner.fetch(sql, *args)
        # Fire on the TARGET STATUS's candidate window only: the prune
        # loop runs one candidate per terminal status, and a fire on an
        # earlier status's window would re-time the row before the
        # succeeded window ever ran (the candidate's own age predicate
        # would then mask the re-check under test).
        if not self.fired and "ORDER BY finished_at" in sql and args and args[0] == "succeeded":
            self.fired = True
            retried = await self._race.fetchrow(render(self._schema).retry_job, self._jid)
            assert retried is not None, "the mid-window retry did not match"
            await self._race.execute(
                f"UPDATE {self._schema}.jobs SET status = 'succeeded'::"
                f"{self._schema}.job_status, finished_at = clock_timestamp() WHERE id = $1",
                self._jid,
            )
        return rows

    async def execute(self, sql: str, *args: object) -> str:
        return await self._inner.execute(sql, *args)


async def test_zero_age_reterminalization_between_window_and_write_is_dropped(
    pg_conn: asyncpg.Connection, settings: TaskQSettings
) -> None:
    """A job retried and re-terminalized INSIDE the batch (between the
    candidate window and the write, deterministically) is terminal again
    but zero seconds old: the lock-time age re-check must keep it out of
    the archive - archiving it would remove a just-finished job from the
    live tables without serving its retention."""
    await _apply(pg_conn, settings)
    schema = settings.schema_name
    await pg_conn.execute(
        f"TRUNCATE {schema}.jobs, {schema}.jobs_archive, "
        f"{schema}.job_attempts, {schema}.job_attempts_archive CASCADE"
    )
    jid = await _seed_terminal_job(
        pg_conn, schema=schema, status="succeeded", finished_at=_NOW - timedelta(days=31)
    )
    race_conn = await asyncpg.connect(str(settings.pg_dsn))
    proxy = _RetermBetweenWindowAndWriteConn(pg_conn, race_conn, schema, jid)
    try:
        result = await prune_terminal_jobs(
            proxy,
            retention_per_status={"succeeded": _FOLD_RETENTION},
            archive_retention=timedelta(days=365),
            schema=schema,
        )
    finally:
        await race_conn.close()

    assert proxy.fired, "the candidate window never ran"
    assert result.total_deleted == 0, "a zero-age re-terminalization was archived"
    live = await pg_conn.fetchrow(
        f"SELECT status, finished_at FROM {schema}.jobs WHERE id = $1", jid
    )
    assert live is not None and live["status"] == "succeeded", (
        "the just-re-terminalized job vanished from the live tables"
    )
    assert _NOW - live["finished_at"] < _FOLD_RETENTION, "the live row is not the fresh version"
    n_archive = await pg_conn.fetchval(
        f"SELECT count(*) FROM {schema}.jobs_archive WHERE id = $1", jid
    )
    assert n_archive == 0
    await assert_master_invariant(pg_conn, schema, retention=_FOLD_RETENTION, seeded={jid})


# ── 3. The write statement binds exactly its id array ────────────────────


async def test_write_statement_binds_exactly_its_id_array(
    pg_conn: asyncpg.Connection, settings: TaskQSettings
) -> None:
    """The bounded-writes discipline, enforced at the SQL itself: the write
    statement moves EXACTLY the ids bound in $3 - a subset candidate cannot
    leak onto eligible rows outside the array, and an empty array moves
    nothing even with a full candidate window waiting."""
    await _apply(pg_conn, settings)
    schema = settings.schema_name
    await pg_conn.execute(
        f"TRUNCATE {schema}.jobs, {schema}.jobs_archive, "
        f"{schema}.job_attempts, {schema}.job_attempts_archive CASCADE"
    )
    ids = await _seed_terminal_jobs_bulk(
        pg_conn, schema, count=5, status="succeeded", finished_at=_NOW - timedelta(days=31)
    )
    write_sql = _ARCHIVE_CTE_SQL.format(schema=schema)
    retention = timedelta(days=30)
    archive_retention = timedelta(days=365)

    # A strict subset bound: exactly those ids move, the rest stay live.
    # (The write CTE's own RETURNING is grouped counts; the moved set is
    # observed through the archive itself.)
    subset = ids[:2]
    rows = await pg_conn.fetch(write_sql, "succeeded", archive_retention, subset, retention)
    assert sum(r["cnt"] for r in rows) == len(subset)
    archived = await pg_conn.fetch(f"SELECT id FROM {schema}.jobs_archive")
    assert {r["id"] for r in archived} == set(subset)
    still_live = await pg_conn.fetch(f"SELECT id FROM {schema}.jobs WHERE status = 'succeeded'")
    assert {r["id"] for r in still_live} == set(ids) - set(subset)

    # An empty array: the window is full but NOTHING may move.
    rows = await pg_conn.fetch(write_sql, "succeeded", archive_retention, [], retention)
    assert rows == []
    assert await pg_conn.fetchval(f"SELECT count(*) FROM {schema}.jobs_archive") == 2

    # The full sweep finishes the rest through the same statement shape.
    result = await prune_terminal_jobs(
        pg_conn,
        retention_per_status={"succeeded": retention},
        archive_retention=archive_retention,
        schema=schema,
    )
    assert result.total_deleted == 3
    await assert_master_invariant(pg_conn, schema, retention=retention, seeded=set(ids))


# ── Timescale container (per module, the established pattern) ────────────

_TIMESCALE_IMAGE = "timescale/timescaledb:2.30.1-pg18"


@pytest.fixture(scope="module")
def timescale_container() -> Iterator[Any]:
    skip_test_without_docker()
    from testcontainers.community.postgres import PostgresContainer

    with PostgresContainer(
        image=_TIMESCALE_IMAGE,
        username="taskq",
        password="taskq",
        dbname="taskq",
    ).with_kwargs(labels=creator_labels()) as container:
        yield container


@pytest.fixture(scope="module")
def timescale_dsn(timescale_container: Any) -> str:
    return timescale_container.get_connection_url().replace(
        "postgresql+psycopg2://", "postgresql://"
    )


@pytest.fixture
def ts_schema() -> str:
    return "ts_atk_" + new_uuid().hex[:12]


# ── 4. Re-archive racing the chunk boundary ──────────────────────────────


async def test_rearchive_racing_chunk_drop_folds_without_duplicates(
    timescale_dsn: str, ts_schema: str
) -> None:
    """The fold runs while a REAL retention-policy run drops the standing
    row's chunk underneath it, across a fleet of ghosts straddling the
    boundary. Whatever order the two mechanisms land in, no id may ever
    hold two archive rows, no live row may survive, and a final drain
    must be a converged no-op (no resurrection, no re-entry)."""
    conn = await asyncpg.connect(timescale_dsn)
    try:
        await _migrate_ts(conn, ts_schema, timescale_dsn, flag=True)
        # Eight ghosts whose standing rows are 3.0-3.8 days old: with 2-day
        # retention and 1-day chunks every standing row's chunk is
        # policy-droppable, and the ages straddle enough chunk boundaries
        # that the background worker and the prune genuinely interleave.
        gids = [
            await _seed_ghost_pair(
                conn,
                ts_schema,
                standing_fin=_NOW - timedelta(days=3.0 + 0.1 * i, hours=12),
                live_fin=_NOW - timedelta(days=3.0 + 0.1 * i, hours=36),
            )
            for i in range(8)
        ]
        # Pull the policies' next_start back to NOW: the chunk drops race
        # the prune from this moment on.
        await _schedule_policies(conn, ts_schema, next_start=datetime.now(UTC))

        for _ in range(3):
            await prune_terminal_jobs(
                conn,
                retention_per_status={"succeeded": _FOLD_RETENTION},
                archive_retention=_FOLD_ARCHIVE_RETENTION,
                schema=ts_schema,
                batch_size=3,  # small batches widen the race window
            )
            for gid in gids:
                facts = await _ghost_facts(conn, ts_schema, gid)
                assert not (facts["live"] and facts["archived"]), (
                    f"a ghost row reappeared live AND archived: {facts}"
                )
                assert facts["archived"] <= 1, (
                    f"an id holds {facts['archived']} archive rows - the "
                    "archive-once guard lost to the chunk-boundary race"
                )

        # Settle: let the policy's own run finish whatever the prune left.
        remaining_live = -1
        for _ in range(60):
            remaining_live = await conn.fetchval(
                f"SELECT count(*) FROM {ts_schema}.jobs WHERE status = 'succeeded'"
            )
            if remaining_live == 0:
                break
            await asyncio.sleep(0.5)
        assert remaining_live == 0, "a ghost's live row survived both mechanisms"

        # Convergence forever after: one more drain is a no-op and nothing
        # resurrects - an id whose standing row was chunk-dropped stays
        # gone; an id that still holds its archive row holds exactly one.
        final = await prune_terminal_jobs(
            conn,
            retention_per_status={"succeeded": _FOLD_RETENTION},
            archive_retention=_FOLD_ARCHIVE_RETENTION,
            schema=ts_schema,
        )
        assert final.total_deleted == 0, "the settled state re-entered the prune window"
        for gid in gids:
            facts = await _ghost_facts(conn, ts_schema, gid)
            assert facts["live"] == 0
            assert facts["archived"] <= 1
        dupes = await conn.fetchval(
            f"SELECT count(*) FROM (SELECT id FROM {ts_schema}.jobs_archive "
            "GROUP BY id HAVING count(*) > 1) d"
        )
        assert dupes == 0, "duplicate archive rows exist somewhere in the archive"
    finally:
        await conn.close()


# ── 5. Expiry sweep vs drop_chunks on the same rows ──────────────────────


async def test_expiry_sweep_vs_chunk_drop_no_double_delete_no_resurrection(
    timescale_dsn: str, ts_schema: str
) -> None:
    """Both retention mechanisms target the same rows simultaneously: the
    sweep deletes by expire_at inside a young chunk, the policy drops an
    old chunk wholesale (hand-set future expire_at stamps force the
    collision - the write's own stamping contract is pinned separately).
    Every row must be deleted exactly once, nothing may resurrect, and a
    final sweep must be a no-op."""
    conn = await asyncpg.connect(timescale_dsn)
    try:
        await _migrate_ts(conn, ts_schema, timescale_dsn, flag=True)
        now = datetime.now(UTC)
        old_fin = now - timedelta(days=3, hours=12)  # policy-droppable chunk
        young_fin = now - timedelta(hours=6)  # young chunk, policy-kept
        seeded: list[Any] = []
        for fin, expire_offset in [
            (old_fin, timedelta(days=1)),  # old chunk, future expire_at
            (old_fin, timedelta(hours=-1)),  # old chunk, past expire_at
            (young_fin, timedelta(hours=-1)),  # young chunk, past expire_at
        ]:
            for _ in range(4):
                jid = new_uuid()
                await conn.execute(
                    f"""INSERT INTO {ts_schema}.jobs_archive (
                        id, actor, queue, payload, max_attempts, retry_kind, status,
                        scheduled_at, schedule_to_close, finished_at, archived_at, expire_at
                    ) VALUES ($1, 'a', 'default', '{{}}'::jsonb, 3, 'transient',
                        'succeeded', $2, $3, $4, $2, $5)""",
                    jid,
                    fin,
                    fin + timedelta(hours=1),
                    fin,
                    now + expire_offset,
                )
                seeded.append(jid)

        await _schedule_policies(conn, ts_schema, next_start=datetime.now(UTC))
        sweep_totals: list[int] = []
        for _ in range(4):
            result = await archive_expiry_sweep(conn, schema=ts_schema, batch_size=3)
            sweep_totals.append(result.total_deleted)
            await asyncio.sleep(0.25)

        # Settle: the policy's chunk drops finish the old-chunk rows.
        remaining = -1
        for _ in range(60):
            remaining = await conn.fetchval(f"SELECT count(*) FROM {ts_schema}.jobs_archive")
            if remaining == 0:
                break
            await asyncio.sleep(0.5)
        assert remaining == 0, f"rows survived both mechanisms: {remaining}"

        # No resurrection: a final sweep and a poll of the table agree.
        final_sweep = await archive_expiry_sweep(conn, schema=ts_schema, batch_size=100)
        assert final_sweep.total_deleted == 0
        assert await conn.fetchval(f"SELECT count(*) FROM {ts_schema}.jobs_archive") == 0
        # The sweep actually raced: the four young-chunk past-expire rows
        # are ONLY reachable by the sweep (their chunk is policy-kept), so
        # its deletions across the rounds are bounded below by them; the
        # four old-chunk past-expire rows it caught depend on how the
        # chunk drop interleaved.
        assert 4 <= sum(sweep_totals) <= 8, f"sweep deletions: {sweep_totals}"
    finally:
        await conn.close()


# ── 6. The chunk-granularity vs expire_at gap, both directions ───────────


def _parse_interval(text: str) -> timedelta:
    """Parse the human interval form timescaledb_information renders
    ('2 days', '6 hours'), never SQL."""
    value, unit = text.strip().split(" ", 1)
    seconds = float(value)
    if unit.startswith("day"):
        return timedelta(days=seconds)
    if unit.startswith("hour"):
        return timedelta(hours=seconds)
    return timedelta(seconds=seconds)


async def test_policy_never_drops_a_chunk_before_its_rows_expire(
    timescale_dsn: str, ts_schema: str
) -> None:
    """The chunk-granularity vs expire_at gap, both directions, verified
    against the LIVE catalogs for rows the REAL archive write stamped.

    The write's stamping contract: expire_at = archived_at +
    archive_retention. Chunk drops key off finished_at: a chunk goes at
    the first policy run T with chunk_end < T - drop_after.

    Direction 1 (documented, BOUNDED): a row can outlive its expire_at
    inside a young chunk - the overshoot is chunk_end - archived_at,
    strictly less than one chunk interval.

    Direction 2 (the docs call hypertable retention 'stricter than
    vanilla expiry by however long the row sat in the hot table'): a
    chunk CAN drop before its rows' expire_at - exactly when the row was
    archived after its finished_at's chunk had elapsed - and the
    early-drop magnitude is strictly bounded by the hot-table latency
    (archived_at - finished_at). This attack DISPROVES the stronger
    'never drops before expire_at' reading: on main the early drop is
    real, deliberate, and bounded.

    The invariant that must actually hold: the earliest chunk-drop
    instant strictly follows finished_at + archive_retention for every
    write-stamped row (retention is never cut short of the job's own
    finish), and the registered drop_after equals the retention the
    write stamps with. A policy registered with a diverged interval cuts
    rows' retention short - the invariant fails on arrival.
    """
    conn = await asyncpg.connect(timescale_dsn)
    try:
        await _migrate_ts(conn, ts_schema, timescale_dsn, flag=True)
        # Eight jobs across more than one chunk, archived by the REAL
        # prune (so expire_at is stamped by the write, not by hand).
        for i in range(8):
            await _seed_terminal_job(
                conn,
                schema=ts_schema,
                status="succeeded",
                finished_at=_NOW - timedelta(hours=6 * i + 6),
            )
        result = await prune_terminal_jobs(
            conn,
            retention_per_status={"succeeded": _FOLD_RETENTION},
            archive_retention=_FOLD_ARCHIVE_RETENTION,
            schema=ts_schema,
        )
        assert result.total_deleted == 8

        policy_row = await conn.fetchrow(
            "SELECT config FROM timescaledb_information.jobs "
            "WHERE hypertable_schema = $1 AND hypertable_name = 'jobs_archive' "
            "AND proc_name = 'policy_retention'",
            ts_schema,
        )
        assert policy_row is not None, "no retention policy registered for jobs_archive"
        config = policy_row["config"]
        if isinstance(config, str):
            config = json.loads(config)
        drop_after = _parse_interval(config["drop_after"])
        assert drop_after == _FOLD_ARCHIVE_RETENTION, (
            f"the policy's drop_after {drop_after} diverges from the retention "
            f"the archive write stamps expire_at with ({_FOLD_ARCHIVE_RETENTION})"
        )

        chunk_interval = await conn.fetchval(
            "SELECT d.interval_length FROM _timescaledb_catalog.hypertable h "
            "JOIN _timescaledb_catalog.dimension d ON d.hypertable_id = h.id "
            "WHERE h.schema_name = $1 AND h.table_name = 'jobs_archive'",
            ts_schema,
        )
        chunk_interval = timedelta(microseconds=chunk_interval)
        rows = await conn.fetch(
            f"SELECT id, finished_at, archived_at, expire_at FROM {ts_schema}.jobs_archive"
        )
        assert len(rows) == 8
        chunks = await conn.fetch(
            "SELECT range_start, range_end FROM timescaledb_information.chunks "
            "WHERE hypertable_schema = $1 AND hypertable_name = 'jobs_archive'",
            ts_schema,
        )
        assert len(chunks) >= 2, "the seeds must span more than one chunk"
        cut_short: list[dict[str, str]] = []
        saw_direction_2 = False
        for row in rows:
            # The stamping contract: the write's expire_at is
            # archive-time plus the retention. archived_at and expire_at
            # are two separate clock_timestamp() evaluations in one
            # target list - two independent server clock reads. The
            # data-safety direction pins tight: the stamp may not cut the
            # promised retention short by more than one straddled clock
            # tick (an early expire_at is what lets the expiry sweep drop
            # a live row). The upper side carries the scheduler's
            # headroom (a late expire_at keeps the row longer - harmless)
            # while still reding a real stamping bug; see
            # _STAMP_GAP_SANITY for the CI evidence.
            stamp_gap = row["expire_at"] - row["archived_at"]
            assert stamp_gap >= _FOLD_ARCHIVE_RETENTION - timedelta(microseconds=1), (
                f"row {row['id']}: expire_at - archived_at = {stamp_gap}, cut "
                f"the promised {_FOLD_ARCHIVE_RETENTION} short - the expiry "
                "sweep could drop this row before its retention ends"
            )
            assert stamp_gap <= _FOLD_ARCHIVE_RETENTION + _STAMP_GAP_SANITY, (
                f"row {row['id']}: expire_at - archived_at = {stamp_gap}, not "
                f"the promised {_FOLD_ARCHIVE_RETENTION} (+{_STAMP_GAP_SANITY} "
                "scheduler headroom) - the write stamps expire_at from "
                "something other than archived_at + retention"
            )
            mine = [c for c in chunks if c["range_start"] <= row["finished_at"] < c["range_end"]]
            assert len(mine) == 1, f"row {row['id']} matches {len(mine)} chunks"
            chunk_end = mine[0]["range_end"]
            earliest_drop = chunk_end + drop_after
            # The invariant that must actually hold: the earliest chunk
            # drop is strictly after finished_at + retention - the job's
            # own finish is never retention-cut-short.
            if earliest_drop <= row["finished_at"] + _FOLD_ARCHIVE_RETENTION:
                cut_short.append(
                    {
                        "id": str(row["id"]),
                        "earliest_drop": str(earliest_drop),
                        "finished_at+retention": str(row["finished_at"] + _FOLD_ARCHIVE_RETENTION),
                    }
                )
            if chunk_end > row["archived_at"]:
                # Direction 1: the row can outlive its expire_at inside
                # its young chunk - bounded by one chunk interval.
                overshoot = chunk_end - row["archived_at"]
                assert overshoot < chunk_interval, (
                    f"row {row['id']} can outlive its expire_at by {overshoot}, "
                    f"past the one-chunk bound {chunk_interval}"
                )
            else:
                # Direction 2: the chunk drops first - the early-drop
                # magnitude is strictly bounded by the hot-table latency.
                saw_direction_2 = True
                early = row["expire_at"] - earliest_drop
                latency = row["archived_at"] - row["finished_at"]
                assert early < latency, (
                    f"row {row['id']} drops {early} before its expire_at, "
                    f"past the hot-table latency bound {latency}"
                )
        assert not cut_short, (
            f"chunks whose rows the policy would drop BEFORE finished_at + retention: {cut_short}"
        )
        # The freshly archived rows sit ~30h after their finished_at, so
        # their 1-day chunks had already elapsed at archive time: the
        # corpus must exercise the early-drop direction, not just the
        # young-chunk one.
        assert saw_direction_2, "the corpus never exercised the early-drop direction"
    finally:
        await conn.close()


# ── 7. Plan truth on the hypertable ──────────────────────────────────────


async def test_expiry_sweep_plan_is_index_bounded_per_chunk_and_windows_prune(
    timescale_dsn: str, ts_schema: str
) -> None:
    """The expiry sweep's plan on the hypertable, and chunk pruning for
    partition-column windows.

    The sweep bounds expire_at, which is NOT the partition column
    (finished_at), so TimescaleDB cannot startup-prune chunks for it - by
    construction, not by neglect. The bound that MUST hold instead: every
    chunk the sweep touches is scanned through the expire_at index with
    the bound as its Index Cond - no per-chunk population walk, the same
    per-chunk guarantee the vanilla pin (test_index_audit) enforces for
    the whole table. A finished_at-windowed read - the partition-column
    shape every archive history consumer uses - must prune chunks for
    real.
    """
    conn = await asyncpg.connect(timescale_dsn)
    try:
        await _migrate_ts(conn, ts_schema, timescale_dsn, flag=True)
        now = datetime.now(UTC)
        for age_days in (3.5, 1.5, 0.5):
            fin = now - timedelta(days=age_days)
            for _ in range(3):
                await conn.execute(
                    f"""INSERT INTO {ts_schema}.jobs_archive (
                        id, actor, queue, payload, max_attempts, retry_kind, status,
                        scheduled_at, schedule_to_close, finished_at, archived_at, expire_at
                    ) VALUES (gen_random_uuid(), 'a', 'default', '{{}}'::jsonb, 3, 'transient',
                        'succeeded', $1, $2, $3, $1, $4)""",
                    fin,
                    fin + timedelta(hours=1),
                    fin,
                    fin + _FOLD_ARCHIVE_RETENTION,
                )
        n_chunks = await conn.fetchval(
            "SELECT count(*) FROM timescaledb_information.chunks "
            "WHERE hypertable_schema = $1 AND hypertable_name = 'jobs_archive'",
            ts_schema,
        )
        assert n_chunks == 3

        plan_rows = await conn.fetch(
            f"EXPLAIN (FORMAT TEXT) {_EXPIRY_CTE_SQL.format(schema=ts_schema)}", 10
        )
        plan = "\n".join(r[0] for r in plan_rows)
        expired_arm = plan.split("CTE expired", 1)[1].split("CTE deleted", 1)[0]
        assert "Seq Scan" not in expired_arm, (
            f"the expiry window walks a chunk population:\n{expired_arm}"
        )
        for line in expired_arm.splitlines():
            if "Scan on _hyper" in line:
                assert "Index Scan" in line and "_jobs_archive_expire_at_idx" in line, (
                    f"a chunk scan in the expiry window is not index-bounded: {line}"
                )
        # Every alive chunk is present in the sweep's plan (it cannot
        # prune them - its bound is not the partition column) and each is
        # index-bounded above.
        expired_chunks = {
            tok
            for line in expired_arm.splitlines()
            for tok in line.split()
            if tok.startswith("_hyper_") and tok.endswith("_chunk")
        }
        assert len(expired_chunks) == 3, (
            f"the expiry window must touch every alive chunk, index-bounded:\n{expired_arm}"
        )

        # Partition-column window: chunk pruning, for real.
        window_rows = await conn.fetch(
            f"""EXPLAIN (FORMAT TEXT) SELECT count(*) FROM "{ts_schema}".jobs_archive
            WHERE finished_at >= $1::timestamptz AND finished_at < $2::timestamptz""",
            now - timedelta(hours=12),
            now,
        )
        window_plan = "\n".join(r[0] for r in window_rows)
        chunk_tokens = {
            tok
            for line in window_plan.splitlines()
            for tok in line.split()
            if tok.startswith("_hyper_") and tok.endswith("_chunk")
        }
        assert chunk_tokens, f"the plan must read chunks:\n{window_plan}"
        assert len(chunk_tokens) < n_chunks, (
            f"a partition-column window must prune at least one chunk "
            f"(read {len(chunk_tokens)} of {n_chunks}):\n{window_plan}"
        )
    finally:
        await conn.close()


# ── 8. The flag boundary on one database ─────────────────────────────────


async def test_flag_flip_cycle_on_one_database_converges(
    timescale_dsn: str, ts_schema: str
) -> None:
    """off -> on -> off -> on on the SAME schema: convergence, no duplicate
    policies, no orphaned chunks, the migration ledger checksums untouched,
    the OFF position a verified ZERO-SQL gate, and the vanilla runtime
    paths unharmed on the still-converted schema after each downgrade
    attempt (runtime code never branches on the mode)."""
    conn = await asyncpg.connect(timescale_dsn)
    try:
        # OFF (virgin): plain migrations, vanilla schema.
        await conn.execute(f'DROP SCHEMA IF EXISTS "{ts_schema}" CASCADE')
        await apply_pending(conn, schema=ts_schema)
        ledger = await conn.fetch(
            f"SELECT version, checksum FROM {ts_schema}.schema_migrations ORDER BY version"
        )
        ht_off = await conn.fetchval(
            "SELECT count(*) FROM _timescaledb_catalog.hypertable WHERE schema_name = $1",
            ts_schema,
        )
        assert ht_off == 0

        # ON.
        on_settings = _ts_settings(timescale_dsn, ts_schema, flag=True)
        report = await enable_hypertables(conn, schema=ts_schema, settings=on_settings)
        assert set(report.converted) == {"job_events", "jobs_archive", "job_attempts_archive"}
        await _schedule_policies(
            conn, ts_schema, next_start=datetime.now(UTC) + timedelta(days=3650)
        )
        jid_on = await _seed_terminal_job(
            conn, schema=ts_schema, status="succeeded", finished_at=_NOW - timedelta(days=31)
        )
        prune_on = await prune_terminal_jobs(
            conn,
            retention_per_status={"succeeded": _FOLD_RETENTION},
            archive_retention=_FOLD_ARCHIVE_RETENTION,
            schema=ts_schema,
        )
        assert prune_on.total_deleted == 1, "the prune must work on the converted schema"

        # OFF (downgrade attempt): the zero-SQL gate, verified against the
        # refusing stub; the schema STAYS converted and the vanilla paths
        # (the same prune, the expiry sweep) keep working on it.
        off_settings = _ts_settings(timescale_dsn, ts_schema, flag=False)
        noop = await enable_hypertables(
            _RefusingConn(),  # pyright: ignore[reportArgumentType]  # Why: the contract under test is that this value is never used.
            schema=ts_schema,
            settings=off_settings,
        )
        assert noop == HypertableReport(converted=(), retention_policies=())
        ht_still = await conn.fetchval(
            "SELECT count(*) FROM _timescaledb_catalog.hypertable WHERE schema_name = $1",
            ts_schema,
        )
        assert ht_still == 3, "a downgrade attempt must not silently un-convert"
        jid_off = await _seed_terminal_job(
            conn, schema=ts_schema, status="failed", finished_at=_NOW - timedelta(days=31)
        )
        prune_off = await prune_terminal_jobs(
            conn,
            retention_per_status={"failed": _FOLD_RETENTION},
            archive_retention=_FOLD_ARCHIVE_RETENTION,
            schema=ts_schema,
        )
        assert prune_off.total_deleted == 1, "the prune broke after a downgrade attempt"
        sweep_off = await archive_expiry_sweep(conn, schema=ts_schema, batch_size=100)
        assert sweep_off.total_deleted >= 0  # runs clean on the converted schema

        # ON again: skip conversion, exactly three policies - no duplicates.
        report2 = await enable_hypertables(conn, schema=ts_schema, settings=on_settings)
        assert report2.converted == (), "an already-converted schema must not re-convert"
        n_policies = await conn.fetchval(
            "SELECT count(*) FROM timescaledb_information.jobs "
            "WHERE hypertable_schema = $1 AND proc_name = 'policy_retention'",
            ts_schema,
        )
        assert n_policies == 3, f"policies accumulated: {n_policies}"

        # OFF once more, then ON (the cycle's second lap).
        await enable_hypertables(_RefusingConn(), schema=ts_schema, settings=off_settings)  # pyright: ignore[reportArgumentType]  # Why: same zero-SQL contract, second lap of the cycle.
        report3 = await enable_hypertables(conn, schema=ts_schema, settings=on_settings)
        assert report3.converted == ()
        n_policies = await conn.fetchval(
            "SELECT count(*) FROM timescaledb_information.jobs "
            "WHERE hypertable_schema = $1 AND proc_name = 'policy_retention'",
            ts_schema,
        )
        assert n_policies == 3

        # No orphaned chunks: every chunk the catalogs show belongs to one
        # of the schema's three hypertables, and show_chunks agrees with
        # the information view.
        info_chunks = await conn.fetchval(
            "SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_schema = $1",
            ts_schema,
        )
        shown = await conn.fetchval(
            "SELECT count(*) FROM ("
            "SELECT show_chunks(format('%I.%I', hypertable_schema, hypertable_name)) AS ch "
            "FROM timescaledb_information.hypertables WHERE hypertable_schema = $1) s",
            ts_schema,
        )
        assert info_chunks == shown, "orphaned chunks: the catalogs disagree"

        # The migration ledger is untouched by the whole cycle.
        ledger_after = await conn.fetch(
            f"SELECT version, checksum FROM {ts_schema}.schema_migrations ORDER BY version"
        )
        assert [tuple(r) for r in ledger_after] == [tuple(r) for r in ledger], (
            "the flag cycle wrote to the migration ledger"
        )
        # And the archive rows the cycle produced are exactly the pruned jobs.
        archive_ids = await conn.fetch(f"SELECT id FROM {ts_schema}.jobs_archive")
        assert {r["id"] for r in archive_ids} == {jid_on, jid_off}
        await assert_master_invariant(
            conn, ts_schema, retention=_FOLD_RETENTION, seeded={jid_on, jid_off}
        )
    finally:
        await conn.close()


# ── 9. The loud refusal leaves the ledger and the prune untouched ────────


async def test_loud_refusal_leaves_ledger_and_prune_untouched(pg_dsn: str) -> None:
    """Opt-in on a vanilla server AFTER the schema migrated: the refusal
    names the setting, converts nothing, writes nothing to the migration
    ledger, and the prune path still converges on the schema afterwards."""
    schema = "ts_atk_refusal"
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema)
        ledger = await conn.fetch(
            f"SELECT version, checksum FROM {schema}.schema_migrations ORDER BY version"
        )
        settings = WorkerSettings.load_from_dict(
            {
                "TASKQ_PG_DSN": pg_dsn,
                "TASKQ_SCHEMA_NAME": schema,
                "TASKQ_TIMESCALEDB_HYPERTABLES": "true",
            }
        )
        with pytest.raises(TimescaleDBUnavailableError, match="TASKQ_TIMESCALEDB_HYPERTABLES"):
            await enable_hypertables(conn, schema=schema, settings=settings)
        ledger_after = await conn.fetch(
            f"SELECT version, checksum FROM {schema}.schema_migrations ORDER BY version"
        )
        assert [tuple(r) for r in ledger_after] == [tuple(r) for r in ledger], (
            "the refused opt-in wrote to the migration ledger"
        )
        # The vanilla prune path is unharmed after the refusal.
        jid = await _seed_terminal_job(
            conn, schema=schema, status="succeeded", finished_at=_NOW - timedelta(days=31)
        )
        result = await prune_terminal_jobs(
            conn,
            retention_per_status={"succeeded": _FOLD_RETENTION},
            archive_retention=timedelta(days=365),
            schema=schema,
        )
        assert result.total_deleted == 1
        await assert_master_invariant(conn, schema, retention=_FOLD_RETENTION, seeded={jid})
    finally:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()

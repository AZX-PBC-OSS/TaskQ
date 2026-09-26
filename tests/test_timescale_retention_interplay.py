# ruff: noqa: S608  # Why: schema is a fixed test identifier, not user input; every value is $-bound.
"""Retention interplay: TaskQ's row-level sweeps vs TimescaleDB chunk drops.

The sibling module (``test_timescaledb_hypertables.py``) pins the conversion
and each mechanism in isolation. This module pins the INTERPLAY: who deletes
historical rows when BOTH the hypertable retention policies (whole-chunk
drops) and TaskQ's own DELETE-based sweeps are armed on the same tables, and
what each mechanism does to the other's bookkeeping. One real
``policy_retention`` run per drop (the ``alter_job`` next_start knob, the
same sanctioned pattern the sibling module pins), so every chunk drop under
test is the registered policy's own work, never a test shortcut.

Five hypotheses, one test each (plus one divergence pin):

1. DOUBLE RETENTION — with the policy AND the expiry/TTL sweeps armed on
   the same table, a sweep that runs after the policy dropped the rows it
   targets is a silent no-op: zero deleted, no error, terminating drain.
   The two mechanisms do not conflict; they compose by (non-)overlap, and
   the post-drop sweep cost is an index probe, not a correction.
2. THE CARVE-OUT — the event-TTL sweep keeps
   ``kind='state_change' AND detail->>'reason'='lock_expired'`` rows to
   ``RECLAIM_OUTBOX_RETENTION_MULTIPLIER`` (100x) the retention, but the
   hypertable policy drops their chunks at PLAIN retention (the policy's
   ``drop_after`` IS ``event_retention_period``). Proven here: the carve-out
   HOLDS inside the sweep (and against it) and is DEFEATED by the policy
   run — an outbox event vanishes at ~1x retention with the sweep never
   having had a say.
3. ARCHIVE CONSISTENCY — ``jobs_archive``'s policy (``drop_after`` =
   ``archive_retention_period`` on ``finished_at``) can drop a chunk while
   its rows' ``expire_at`` stamps are still far in the future: the expiry
   sweep (which honors ``expire_at`` exactly) keeps the row, the policy
   drop removes it ~300 days early. Chunk granularity, not ``expire_at``,
   governs the aged end once the policy is armed.
4. THE SWEEPS' BOOKKEEPING — after a policy drop removes a swath of rows,
   the next sweep's batch math behaves: the drained-until-zero loop
   terminates on the empty window, the ``batch_total < size`` boundary
   stays exact, and — the sharp edge — the event-prune watermark
   (``job_events_prune_state.pruned_through_id``, the fail-visible
   ``EventRetentionGapError`` bound) does NOT advance on a chunk drop, so
   the "watermark can never lag what is already gone" contract migration
   01.00.20_02 states for the two sweep deleters does not extend to the
   third deleter (the policy). Pinned as the behavior it is: a consumer
   cursor below the dropped ids silently loses them.
5. ENGINE PARITY — with policies present but not fired, the same sweep
   script (prune → archive expiry → event TTL) produces IDENTICAL
   observable outcomes on vanilla Postgres and the hypertable mode: same
   counts, same survivors, same watermark. The sweeps' contracts hold on
   both storage engines.

Plus one pinned divergence (H6): the archive write's NOT EXISTS fold guard
("a job id is archived at most once") is keyed on the archive row's
EXISTENCE, and a chunk drop removes that witness — after the ghost's chunk
drops, the hypertable mode re-archives a retried job where vanilla would
fold, so the once-only archive invariant holds only while archive chunks
live.

Chunk-drop waits poll TimescaleDB's background worker (an external daemon
no test clock can advance); the poll is bounded and asserts on the
policy's own committed effect, mirroring the sibling module's pattern.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.constants import RECLAIM_OUTBOX_RETENTION_MULTIPLIER
from taskq.migrate import apply_pending
from taskq.settings import WorkerSettings
from taskq.testing._shared_containers import creator_labels, skip_test_without_docker
from taskq.timescale import enable_hypertables
from taskq.worker.leader import archive_expiry_sweep, prune_terminal_jobs

pytestmark = pytest.mark.integration

#: Chunk interval the short test retentions clamp to (retention/4 clamped to
#: the 1-day floor): every aged seed below lands a full chunk interval clear
#: of its policy's drop boundary.
_TEST_CHUNK_INTERVAL = timedelta(days=1)
_TEST_ARCHIVE_RETENTION = timedelta(days=2)
_TEST_EVENT_RETENTION = timedelta(days=1)

#: Ages with >= half a chunk of margin inside their policy's drop boundary:
#: a chunk's END time must sit strictly below ``now - drop_after`` for the
#: policy to drop it, so the seeds age half a chunk beyond the boundary
#: instead of sitting exactly on it.
_AGED_ARCHIVE_FINISHED_AT = timedelta(days=3.5)  # boundary: 2 days
_AGED_EVENT_OCCURRENCE = timedelta(days=2.5)  # boundary: 1 day

_TIMESCALE_IMAGE_DEFAULT = "timescale/timescaledb:2.30.1-pg18"
_TIMESCALE_IMAGE = os.environ.get("TASKQ_TEST_TIMESCALEDB_IMAGE") or _TIMESCALE_IMAGE_DEFAULT


# ── Fixtures and seed helpers ─────────────────────────────────────────────


@pytest.fixture(scope="module")
def timescale_container() -> Iterator[Any]:
    """One timescaledb container per module; skips without Docker."""
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
    """The module container's DSN in asyncpg form."""
    return timescale_container.get_connection_url().replace(
        "postgresql+psycopg2://", "postgresql://"
    )


@pytest.fixture
def ts_schema() -> str:
    """A unique schema name per test (the container's lifetime bounds it)."""
    return "tsr_" + new_uuid().hex[:12]


def _ts_settings(dsn: str, schema: str) -> WorkerSettings:
    """Flag on, short retentions (chunk interval clamps to its 1-day floor)."""
    return WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": dsn,
            "TASKQ_SCHEMA_NAME": schema,
            "TASKQ_TIMESCALEDB_HYPERTABLES": "true",
            "TASKQ_ARCHIVE_RETENTION_PERIOD": f"{int(_TEST_ARCHIVE_RETENTION.total_seconds())}s",
            "TASKQ_EVENT_RETENTION_PERIOD": f"{int(_TEST_EVENT_RETENTION.total_seconds())}s",
        }
    )


async def _migrate(conn: asyncpg.Connection, schema: str) -> None:
    await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    await apply_pending(conn, schema=schema)


@pytest.fixture
async def ts_conn(
    timescale_dsn: str,
    ts_schema: str,
) -> AsyncIterator[asyncpg.Connection]:
    """A connection to a freshly migrated AND converted schema.

    Policies are DEFERRED to the far future at setup: a ~now-scheduled
    first policy run could land between a test's seeds and its
    pre-policy assertions. Tests that need a drop pull next_start back to
    now explicitly (``_force_policies_now``), so what they assert stays
    the policy's own run on the test's clock.
    """
    conn = await asyncpg.connect(timescale_dsn)
    try:
        await _migrate(conn, ts_schema)
        report = await enable_hypertables(
            conn, schema=ts_schema, settings=_ts_settings(timescale_dsn, ts_schema)
        )
        assert set(report.converted) == {"job_events", "jobs_archive", "job_attempts_archive"}
        await _schedule_policies(
            conn, ts_schema, next_start=datetime.now(UTC) + timedelta(days=3650)
        )
        yield conn
    finally:
        await conn.close()


async def _schedule_policies(
    conn: asyncpg.Connection, schema: str, *, next_start: datetime
) -> None:
    """Move every retention policy's next run to *next_start* (the supported
    ``alter_job`` knob); the background worker executes the registered
    policy itself, so a drop under test is always a real policy run."""
    rows = await conn.fetch(
        "SELECT job_id FROM timescaledb_information.jobs "
        "WHERE hypertable_schema = $1 AND proc_name = 'policy_retention'",
        schema,
    )
    for r in rows:
        await conn.execute(
            "SELECT alter_job($1, next_start => $2::timestamptz)",
            r["job_id"],
            next_start,
        )


async def _force_policies_now(conn: asyncpg.Connection, schema: str) -> None:
    """Pull every retention policy's next run to now (a real policy run)."""
    await _schedule_policies(conn, schema, next_start=datetime.now(UTC))


async def _wait_for(
    condition: Callable[[], Awaitable[bool]], *, what: str, timeout_secs: float = 60.0
) -> None:
    """Bounded poll for an effect of TimescaleDB's background worker.

    The worker is an external daemon no test clock can advance; the
    sibling module's policy legs use this exact pattern (``alter_job``
    next_start, then poll for the policy's own committed effect). The
    poll never substitutes for a synchronization the test controls —
    every awaited statement outside it is fully ordered.
    """
    deadline = time.monotonic() + timeout_secs
    while time.monotonic() < deadline:
        if await condition():
            return
        await asyncio.sleep(0.5)
    raise AssertionError(f"timed out waiting for {what}")


async def _chunk_names(conn: asyncpg.Connection, schema: str, table: str) -> list[str]:
    rows = await conn.fetch(f'SELECT show_chunks(\'"{schema}"."{table}"\') AS ch')
    return sorted(r["ch"] for r in rows)


async def _chunks_dropped(
    conn: asyncpg.Connection, schema: str, table: str, expect_max: int
) -> bool:
    """Whether the table is down to at most *expect_max* chunks."""
    return len(await _chunk_names(conn, schema, table)) <= expect_max


async def _seed_parent_job(conn: asyncpg.Connection, schema: str, jid: Any) -> None:
    now = datetime.now(UTC)
    await conn.execute(
        f"""INSERT INTO {schema}.jobs (id, actor, queue, payload, max_attempts,
            retry_kind, status, scheduled_at, schedule_to_close)
        VALUES ($1, 'test_actor', 'default', '{{"v":1}}'::jsonb, 3, 'transient',
            'succeeded', $2, $3)""",
        jid,
        now,
        now + timedelta(hours=1),
    )


async def _seed_event(
    conn: asyncpg.Connection,
    schema: str,
    *,
    occurred_at: datetime,
    detail: dict[str, Any],
) -> int:
    """One event on a fresh parent job; returns the event id.

    job_events carries a FK to jobs, so every event needs its parent row.
    """
    jid = new_uuid()
    await _seed_parent_job(conn, schema, jid)
    row = await conn.fetchrow(
        f"INSERT INTO {schema}.job_events (job_id, occurred_at, kind, detail) "
        "VALUES ($1, $2, 'state_change', $3::jsonb) RETURNING id",
        jid,
        occurred_at,
        json.dumps(detail),
    )
    return int(row["id"])


async def _seed_archive_row(
    conn: asyncpg.Connection,
    schema: str,
    *,
    finished_at: datetime,
    expire_at: datetime,
    payload: dict[str, Any] | None = None,
) -> Any:
    """One jobs_archive row with explicit finished_at and expire_at clocks."""
    jid = new_uuid()
    now = datetime.now(UTC)
    await conn.execute(
        f"""INSERT INTO {schema}.jobs_archive (
            id, actor, queue, payload, max_attempts, retry_kind, status,
            scheduled_at, schedule_to_close, finished_at, archived_at, expire_at
        ) VALUES ($1, 'test_actor', 'default', $2::jsonb, 3, 'transient',
            'succeeded', $3, $4, $5, $3, $6)""",
        jid,
        json.dumps(payload if payload is not None else {"v": 1}),
        now,
        now + timedelta(hours=1),
        finished_at,
        expire_at,
    )
    return jid


async def _event_watermark(conn: asyncpg.Connection, schema: str) -> int:
    value = await conn.fetchval(
        f"SELECT pruned_through_id FROM {schema}.job_events_prune_state WHERE singleton = true"
    )
    return int(value)


# ── H1: double retention — the sweeps after a chunk drop ─────────────────


async def test_sweeps_after_chunk_drop_are_silent_noops(
    ts_conn: asyncpg.Connection, ts_schema: str
) -> None:
    """H1: policy drops + TaskQ's sweeps armed on the SAME tables do not
    conflict — each sweep that runs after the drop is a silent, terminating
    no-op over the dropped range.

    Both mechanisms target the same rows here (the aged archive rows are
    past ``expire_at`` AND inside a chunk past ``drop_after``; the aged
    event is past the event retention AND in a dropped chunk). The policy
    run deletes them first; the sweeps then run their real SQL against the
    empty range and must return zero deleted — no error, no partial state,
    fresh rows untouched. The deletion story on a hypertable schema is
    therefore: the policy owns the aged end, and the sweeps' scheduled
    runs over it cost an index probe (wasted work, never a conflict).
    """
    now = datetime.now(UTC)
    fresh_id = await _seed_archive_row(
        ts_conn,
        ts_schema,
        finished_at=now,
        expire_at=now + timedelta(days=365),
    )
    await _seed_archive_row(
        ts_conn,
        ts_schema,
        finished_at=now - _AGED_ARCHIVE_FINISHED_AT,
        expire_at=now - timedelta(hours=1),  # past expiry too: both mechanisms eligible
    )
    await _seed_archive_row(
        ts_conn,
        ts_schema,
        finished_at=now - _AGED_ARCHIVE_FINISHED_AT,
        expire_at=now - timedelta(hours=1),
    )
    aged_event_id = await _seed_event(
        ts_conn,
        ts_schema,
        occurred_at=now - _AGED_EVENT_OCCURRENCE,
        detail={},
    )
    fresh_event_id = await _seed_event(ts_conn, ts_schema, occurred_at=now, detail={})

    assert len(await _chunk_names(ts_conn, ts_schema, "jobs_archive")) == 2
    assert len(await _chunk_names(ts_conn, ts_schema, "job_events")) == 2

    await _force_policies_now(ts_conn, ts_schema)
    await _wait_for(
        lambda: _chunks_dropped(ts_conn, ts_schema, "jobs_archive", 1),
        what="the aged jobs_archive chunk to be dropped by the policy",
    )
    await _wait_for(
        lambda: _chunks_dropped(ts_conn, ts_schema, "job_events", 1),
        what="the aged job_events chunk to be dropped by the policy",
    )

    # Both sweeps run their real statements over the dropped range.
    expiry = await archive_expiry_sweep(ts_conn, schema=ts_schema, batch_size=100)
    assert expiry.total_deleted == 0, (
        "the expiry sweep over chunk-dropped rows must be a no-op, not an error"
    )
    from taskq.backend._sweeps import sweep_expired_events

    deleted = await sweep_expired_events(
        ts_conn, schema=ts_schema, retention=_TEST_EVENT_RETENTION, batch_size=100
    )
    assert deleted == 0, "the event TTL sweep over chunk-dropped rows must be a no-op"

    # The survivors are exactly the fresh rows: the policy deleted only the
    # aged chunks' contents, the sweeps deleted nothing at all.
    remaining_archive = await ts_conn.fetch(f"SELECT id FROM {ts_schema}.jobs_archive")
    assert {r["id"] for r in remaining_archive} == {fresh_id}
    remaining_events = await ts_conn.fetch(f"SELECT id FROM {ts_schema}.job_events")
    assert {int(r["id"]) for r in remaining_events} == {fresh_event_id}
    assert aged_event_id not in {int(r["id"]) for r in remaining_events}


# ── H2: the reclaim-outbox carve-out vs the chunk policy ─────────────────


async def test_reclaim_carveout_holds_in_sweep_and_falls_to_chunk_policy(
    ts_conn: asyncpg.Connection, ts_schema: str
) -> None:
    """H2: the crash-reclaim outbox carve-out is real in the sweep and
    DEFEATED by the hypertable policy.

    With policies not yet fired, the event TTL sweep must honor the
    carve-out on the hypertable exactly as on vanilla: the aged
    ``lock_expired`` event (2.5 days, far inside its 100x = 100-day
    carve-out window) survives the sweep while its aged ordinary sibling
    is deleted. Then ONE real policy run drops the aged chunk outright:
    the same event the sweep just spared vanishes at ~plain retention.
    The policy's ``drop_after`` IS ``event_retention_period``, so on a
    hypertable schema the carve-out's 100x window can never be honored
    past roughly one chunk interval — the sweep's promise and the
    policy's behavior disagree, and the policy wins.
    """
    now = datetime.now(UTC)
    aged_outbox_id = await _seed_event(
        ts_conn,
        ts_schema,
        occurred_at=now - _AGED_EVENT_OCCURRENCE,
        detail={"reason": "lock_expired"},
    )
    aged_ordinary_id = await _seed_event(
        ts_conn,
        ts_schema,
        occurred_at=now - _AGED_EVENT_OCCURRENCE,
        detail={},
    )
    fresh_outbox_id = await _seed_event(
        ts_conn,
        ts_schema,
        occurred_at=now,
        detail={"reason": "lock_expired"},
    )

    from taskq.backend._sweeps import sweep_expired_events

    deleted = await sweep_expired_events(
        ts_conn, schema=ts_schema, retention=_TEST_EVENT_RETENTION, batch_size=100
    )
    assert deleted == 1, "only the aged ordinary event is past retention"
    surviving = {
        int(r["id"]) for r in await ts_conn.fetch(f"SELECT id FROM {ts_schema}.job_events")
    }
    assert aged_outbox_id in surviving, (
        "the carve-out must hold in the sweep on the hypertable: the aged "
        "lock_expired event is far inside its 100x window"
    )
    assert aged_ordinary_id not in surviving
    assert fresh_outbox_id in surviving

    # One real policy run: the aged chunk (the outbox event included) drops.
    await _force_policies_now(ts_conn, ts_schema)
    await _wait_for(
        lambda: _chunks_dropped(ts_conn, ts_schema, "job_events", 1),
        what="the aged job_events chunk to be dropped by the policy",
    )

    surviving = {
        int(r["id"]) for r in await ts_conn.fetch(f"SELECT id FROM {ts_schema}.job_events")
    }
    assert aged_outbox_id not in surviving, (
        "the chunk policy defeats the carve-out: the lock_expired event "
        f"the sweep kept to {RECLAIM_OUTBOX_RETENTION_MULTIPLIER}x retention "
        "is dropped at plain retention by the policy"
    )
    assert surviving == {fresh_outbox_id}


# ── H3: expire_at vs the policy's partition-column clock ─────────────────


async def test_chunk_policy_drops_archive_rows_before_expire_at(
    ts_conn: asyncpg.Connection, ts_schema: str
) -> None:
    """H3: a chunk drop removes archived rows BEFORE the archive-expiry
    sweep expected them.

    The expiry sweep deletes exactly on ``expire_at``; the policy drops on
    ``finished_at`` age. This row is 3.5 days old (past the 2-day
    ``drop_after``) with ``expire_at`` ~300 days in the future — the
    ordinary shape when a job sat terminal in the hot table well past the
    archive interval before its daily prune, or when the operator shortened
    ``archive_retention_period`` after rows already carry long stamps.

    Order of proof: the expiry sweep runs FIRST and honors ``expire_at``
    exactly (keeps the row); the policy run then drops the row's chunk
    outright; the expiry sweep runs again, finds nothing, and terminates.
    On a hypertable schema with policies armed, ``expire_at`` is honored
    only inside chunk lifetime — the aged end answers to the partition
    column, not to the stamp.
    """
    now = datetime.now(UTC)
    jid = await _seed_archive_row(
        ts_conn,
        ts_schema,
        finished_at=now - _AGED_ARCHIVE_FINISHED_AT,
        expire_at=now + timedelta(days=300),
    )

    expiry = await archive_expiry_sweep(ts_conn, schema=ts_schema, batch_size=100)
    assert expiry.total_deleted == 0, (
        "the expiry sweep must honor expire_at exactly: the stamp is far in "
        "the future, the row stays"
    )
    n = await ts_conn.fetchval(f"SELECT count(*) FROM {ts_schema}.jobs_archive WHERE id = $1", jid)
    assert n == 1

    await _force_policies_now(ts_conn, ts_schema)
    await _wait_for(
        lambda: _chunks_dropped(ts_conn, ts_schema, "jobs_archive", 0),
        what="the row's whole chunk to be dropped by the policy",
    )

    n = await ts_conn.fetchval(f"SELECT count(*) FROM {ts_schema}.jobs_archive WHERE id = $1", jid)
    assert n == 0, (
        "the policy dropped the chunk ~300 days before the row's expire_at: "
        "the expiry sweep never got its expected chance at the row"
    )
    expiry = await archive_expiry_sweep(ts_conn, schema=ts_schema, batch_size=100)
    assert expiry.total_deleted == 0, (
        "the sweep after the drop terminates cleanly on the empty range"
    )


# ── H4: the sweeps' bookkeeping after a chunk drop ────────────────────────


async def test_batch_math_stays_exact_after_partial_chunk_drop(
    ts_conn: asyncpg.Connection, ts_schema: str
) -> None:
    """H4a: the expiry drain's batch math behaves when a policy drop has
    removed a swath of its target rows.

    Twelve expired rows across two chunks; the policy drops the aged
    chunk (6 rows) and keeps the fresh one. The next drain at batch size 4
    must delete EXACTLY the 6 surviving rows — 4, then 2 (the
    ``batch_total < size`` boundary breaks the loop), then stop on the
    empty window — and terminate. A spin, an over-delete, or an error is
    the failure this test exists to catch (a spin trips the suite's test
    timeout).
    """
    now = datetime.now(UTC)
    for _ in range(6):
        await _seed_archive_row(
            ts_conn,
            ts_schema,
            finished_at=now - _AGED_ARCHIVE_FINISHED_AT,
            expire_at=now - timedelta(hours=1),
        )
    for _ in range(6):
        await _seed_archive_row(
            ts_conn,
            ts_schema,
            finished_at=now,
            expire_at=now - timedelta(hours=1),
        )

    await _force_policies_now(ts_conn, ts_schema)
    await _wait_for(
        lambda: _chunks_dropped(ts_conn, ts_schema, "jobs_archive", 1),
        what="the aged jobs_archive chunk to be dropped by the policy",
    )

    expiry = await archive_expiry_sweep(ts_conn, schema=ts_schema, batch_size=4)
    assert expiry.total_deleted == 6, (
        "the drain must delete exactly the survivors of the chunk drop: "
        "batches 4 then 2, the < size boundary ending the loop"
    )
    assert expiry.by_status == {"succeeded": 6}
    n = await ts_conn.fetchval(f"SELECT count(*) FROM {ts_schema}.jobs_archive")
    assert n == 0


async def test_chunk_drop_does_not_advance_the_event_watermark(
    ts_conn: asyncpg.Connection, ts_schema: str
) -> None:
    """H4b: a chunk drop deletes events WITHOUT advancing the event-prune
    watermark — the third deleter bypasses the fail-visible contract.

    Migration 01.00.20_02's contract: each event deleter advances
    ``job_events_prune_state.pruned_through_id`` in the same transaction as
    its delete, so a ``watch_reclaims`` consumer with a cursor strictly
    below the watermark gets a loud ``EventRetentionGapError`` instead of a
    silent skip. The two sweep deleters honor it (pinned elsewhere). The
    POLICY deleter cannot: a chunk drop is a catalog operation, it runs no
    TaskQ statement, so the watermark stays put while rows vanish.

    Pinned here with zero sweeps in play: aged events drop with their
    chunk, the watermark never moves from its migration default, and a
    consumer cursor at any position below the dropped ids reads as "safe"
    (cursor >= watermark) while the events it wants are already gone —
    silent loss, exactly the shape the watermark exists to prevent.
    """
    now = datetime.now(UTC)
    dropped_ids = [
        await _seed_event(
            ts_conn,
            ts_schema,
            occurred_at=now - _AGED_EVENT_OCCURRENCE,
            detail={},
        ),
        await _seed_event(
            ts_conn,
            ts_schema,
            occurred_at=now - _AGED_EVENT_OCCURRENCE,
            detail={"reason": "lock_expired"},
        ),
    ]
    fresh_id = await _seed_event(ts_conn, ts_schema, occurred_at=now, detail={})

    watermark_before = await _event_watermark(ts_conn, ts_schema)

    await _force_policies_now(ts_conn, ts_schema)
    await _wait_for(
        lambda: _chunks_dropped(ts_conn, ts_schema, "job_events", 1),
        what="the aged job_events chunk to be dropped by the policy",
    )

    surviving = {
        int(r["id"]) for r in await ts_conn.fetch(f"SELECT id FROM {ts_schema}.job_events")
    }
    assert fresh_id in surviving
    assert all(did not in surviving for did in dropped_ids), "the policy dropped the aged events"

    watermark_after = await _event_watermark(ts_conn, ts_schema)
    assert watermark_after == watermark_before, (
        "the chunk drop must not write anything: the watermark stays where the sweeps left it"
    )
    assert watermark_after < max(dropped_ids), (
        "a consumer cursor anywhere below the dropped ids is >= the "
        "unchanged watermark, so the poll's fail-visible gap check never "
        "fires: the loss is silent"
    )


# ── H5: engine parity — the sweep contracts hold on both engines ──────────


async def _parity_scenario(conn: asyncpg.Connection, schema: str) -> dict[str, Any]:
    """One sweep script, run identically on both engines.

    Plants: three aged terminal jobs (with an attempt and one event each —
    the prune's archive-and-delete plus its event cascade), three expired
    archive rows (the expiry sweep's targets), three aged ordinary events
    and one aged + one fresh lock_expired event on FRESH parents (the
    event TTL sweep's targets and its carve-out). Runs prune (batch 2),
    then archive expiry (batch 4), then the event TTL sweep. Returns every
    observable the sweeps publish.
    """
    now = datetime.now(UTC)
    aged = datetime.now(UTC) - timedelta(days=31)

    for _ in range(3):
        jid = new_uuid()
        await conn.execute(
            f"""INSERT INTO {schema}.jobs (id, actor, queue, payload, max_attempts,
                retry_kind, status, scheduled_at, schedule_to_close, finished_at)
            VALUES ($1, 'test_actor', 'default', '{{"v":1}}'::jsonb, 3, 'transient',
                'succeeded', $2, $3, $4)""",
            jid,
            aged,
            aged + timedelta(hours=1),
            aged,
        )
        await conn.execute(
            f"""INSERT INTO {schema}.job_attempts (job_id, attempt, started_at,
                finished_at, outcome, metadata)
            VALUES ($1, 1, $2, $3, 'succeeded', '{{}}'::jsonb)""",
            jid,
            aged,
            aged + timedelta(minutes=4),
        )
        await conn.execute(
            f"INSERT INTO {schema}.job_events (job_id, occurred_at, kind) "
            "VALUES ($1, $2, 'state_change')",
            jid,
            aged,
        )

    for _ in range(3):
        await _seed_archive_row(
            conn,
            schema,
            finished_at=now,
            expire_at=now - timedelta(hours=1),
        )

    aged_ordinary_ids = [
        await _seed_event(conn, schema, occurred_at=aged, detail={}) for _ in range(3)
    ]
    aged_outbox_id = await _seed_event(
        conn, schema, occurred_at=aged, detail={"reason": "lock_expired"}
    )
    fresh_outbox_id = await _seed_event(
        conn, schema, occurred_at=now, detail={"reason": "lock_expired"}
    )

    from taskq.backend._sweeps import sweep_expired_events

    prune = await prune_terminal_jobs(
        conn,
        retention_per_status={"succeeded": timedelta(days=30)},
        archive_retention=timedelta(days=365),
        schema=schema,
        batch_size=2,
    )
    expiry = await archive_expiry_sweep(conn, schema=schema, batch_size=4)
    events_deleted = await sweep_expired_events(
        conn, schema=schema, retention=timedelta(days=1), batch_size=10
    )

    return {
        "prune_total_deleted": prune.total_deleted,
        "prune_by_status": dict(prune.by_status),
        "expiry_total_deleted": expiry.total_deleted,
        "expiry_by_status": dict(expiry.by_status),
        "events_deleted": events_deleted,
        "jobs_left": await conn.fetchval(f"SELECT count(*) FROM {schema}.jobs"),
        "archive_rows": await conn.fetchval(f"SELECT count(*) FROM {schema}.jobs_archive"),
        "attempts_archive_rows": await conn.fetchval(
            f"SELECT count(*) FROM {schema}.job_attempts_archive"
        ),
        "event_ids_left": sorted(
            int(r["id"]) for r in await conn.fetch(f"SELECT id FROM {schema}.job_events")
        ),
        "watermark": await _event_watermark(conn, schema),
        # Witness ids (the carve-out's survivors on both engines must be
        # exactly these two).
        "aged_ordinary_ids": sorted(aged_ordinary_ids),
        "aged_outbox_id": aged_outbox_id,
        "fresh_outbox_id": fresh_outbox_id,
    }


async def test_sweep_script_agrees_across_both_engines(timescale_dsn: str, pg_dsn: str) -> None:
    """H5: the sweeps' contracts hold on BOTH storage engines.

    The same script on vanilla Postgres and on the hypertable mode
    (policies present, deferred to the far future so no drop interferes):
    identical prune counts, identical expiry counts, identical event-TTL
    counts including the carve-out, identical survivors, identical
    watermark. The sweeps' SQL is mode-agnostic — no runtime code branches
    on hypertables, and the observable outcomes prove it end to end.
    """
    outcomes: dict[str, dict[str, Any]] = {}
    for label, dsn in (("vanilla", pg_dsn), ("timescale", timescale_dsn)):
        schema = f"tsr_parity_{label}"
        conn = await asyncpg.connect(dsn)
        try:
            await _migrate(conn, schema)
            if label == "timescale":
                await enable_hypertables(conn, schema=schema, settings=_ts_settings(dsn, schema))
                # Defer the policies past the scenario: parity is about the
                # sweeps, not the drops (H1-H4 own those).
                await _schedule_policies(
                    conn, schema, next_start=datetime.now(UTC) + timedelta(days=3650)
                )
            outcomes[label] = await _parity_scenario(conn, schema)
        finally:
            await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            await conn.close()

    vanilla = outcomes["vanilla"]
    timescale = outcomes["timescale"]
    assert vanilla == timescale, f"engines disagree:\nvanilla={vanilla}\ntimescale={timescale}"

    # The shared contract, spelled out: prune archived and deleted all three
    # aged jobs (batches 2+2+1, the < size boundary), their events cascaded;
    # expiry deleted the three expired archive rows; the event sweep deleted
    # the three aged ordinary events and honored the carve-out (the aged
    # lock_expired survives beside the fresh one); the watermark advanced to
    # the highest id either deleter removed.
    assert vanilla["prune_total_deleted"] == 3
    assert vanilla["prune_by_status"] == {"succeeded": 3}
    assert vanilla["expiry_total_deleted"] == 3
    assert vanilla["events_deleted"] == 3
    assert vanilla["jobs_left"] == 5, (
        "only the three aged terminal jobs are pruned; the five fresh event-parent jobs stay"
    )
    assert vanilla["archive_rows"] == 3
    assert vanilla["attempts_archive_rows"] == 3
    assert set(vanilla["event_ids_left"]) == {vanilla["aged_outbox_id"], vanilla["fresh_outbox_id"]}
    assert vanilla["watermark"] >= max(vanilla["aged_ordinary_ids"]), (
        "the watermark advanced over every id the sweeps deleted"
    )
    assert vanilla["watermark"] < vanilla["fresh_outbox_id"]


# ── H6: the fold guard loses its witness to a chunk drop (pinned divergence)


async def test_chunk_dropped_archive_row_loses_the_fold_guard(
    timescale_dsn: str, pg_dsn: str
) -> None:
    """H6: "a job id is archived at most once" holds only while archive
    chunks live.

    The archive write's NOT EXISTS guard is keyed on the archive row's
    EXISTENCE. Plant the pre-fix ghost (an archive row whose job is live
    and aged again) with the ghost row INSIDE a policy-droppable chunk:

    * vanilla: the guard folds the re-archive — the standing ghost row is
      the only archive copy, and the live row is removed either way.
    * hypertable, after the ghost's chunk dropped: the guard's witness is
      gone, so the prune re-archives the job — a SECOND archive lifetime
      with a fresh ``archived_at``/``expire_at`` (retention extension) and
      a payload restored from the live row, not the original.

    This test PINS the divergence as the documented behavior it is: the
    once-only archive invariant is engine-conditioned on the hypertable
    mode, exactly when a policy drop races a retried job's re-prune.
    """
    outcomes: dict[str, dict[str, Any]] = {}
    for label, dsn in (("vanilla", pg_dsn), ("timescale", timescale_dsn)):
        schema = f"tsr_fold_{label}"
        conn = await asyncpg.connect(dsn)
        try:
            await _migrate(conn, schema)
            if label == "timescale":
                await enable_hypertables(conn, schema=schema, settings=_ts_settings(dsn, schema))
                await _schedule_policies(
                    conn, schema, next_start=datetime.now(UTC) + timedelta(days=3650)
                )

            now = datetime.now(UTC)
            aged = datetime.now(UTC) - timedelta(days=31)
            # The live row: terminal, aged past the prune retention.
            ghost = new_uuid()
            await conn.execute(
                f"""INSERT INTO {schema}.jobs (id, actor, queue, payload, max_attempts,
                    retry_kind, status, scheduled_at, schedule_to_close, finished_at)
                VALUES ($1, 'test_actor', 'default', '{{"v":1}}'::jsonb, 3, 'transient',
                    'succeeded', $2, $3, $4)""",
                ghost,
                aged,
                aged + timedelta(hours=1),
                aged,
            )
            # The ghost archive row: same id, marked, finished_at inside a
            # policy-droppable chunk (3.5 days old, 2-day drop_after).
            await conn.execute(
                f"""INSERT INTO {schema}.jobs_archive (id, actor, queue, payload,
                    max_attempts, retry_kind, status, scheduled_at, schedule_to_close,
                    finished_at, archived_at, expire_at)
                VALUES ($1, 'test_actor', 'default', '{{"ghost": true}}'::jsonb, 3,
                    'transient', 'succeeded', $2, $3, $4, $5, $6)""",
                ghost,
                aged,
                aged + timedelta(hours=1),
                now - _AGED_ARCHIVE_FINISHED_AT,
                now - timedelta(days=400),
                now - timedelta(days=35),
            )

            if label == "timescale":
                # Drop the ghost's chunk: the guard's witness is gone.
                await _force_policies_now(conn, schema)
                await _wait_for(
                    # Default args bind the loop variables (B023): the
                    # lambda runs before the loop's next iteration, but the
                    # linter cannot prove the await ordering.
                    lambda c=conn, s=schema: _chunks_dropped(c, s, "jobs_archive", 0),
                    what="the ghost's chunk to be dropped by the policy",
                )

            result = await prune_terminal_jobs(
                conn,
                retention_per_status={"succeeded": timedelta(days=30)},
                archive_retention=timedelta(days=365),
                schema=schema,
                batch_size=100,
            )
            rows = await conn.fetch(f"SELECT id, payload, archived_at FROM {schema}.jobs_archive")
            live = await conn.fetchval(f"SELECT count(*) FROM {schema}.jobs WHERE id = $1", ghost)
            outcomes[label] = {
                "prune_deleted": result.total_deleted,
                "archive_rows": len(rows),
                "ghost_payload_wins": any(
                    r["id"] == ghost and json.loads(r["payload"]).get("ghost") is True for r in rows
                ),
                "ghost_live_copies": live,
            }
        finally:
            await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            await conn.close()

    vanilla = outcomes["vanilla"]
    timescale = outcomes["timescale"]

    # Vanilla: the fold — the standing ghost row wins, one archive copy.
    assert vanilla["archive_rows"] == 1
    assert vanilla["ghost_payload_wins"] is True, "the NOT EXISTS guard folds the re-archive"
    assert vanilla["ghost_live_copies"] == 0
    assert vanilla["prune_deleted"] == 1

    # Hypertable after the drop: the guard's witness is gone, the prune
    # re-archives — the once-only invariant does NOT hold. Pinned as the
    # divergence: same id, fresh archive lifetime, original payload restored.
    assert timescale["archive_rows"] == 1
    assert timescale["ghost_payload_wins"] is False, (
        "after the ghost's chunk dropped, the guard cannot fold: the prune "
        "re-archived the job from its live row (fresh payload, fresh stamps)"
    )
    assert timescale["ghost_live_copies"] == 0
    assert timescale["prune_deleted"] == 1
    assert vanilla != timescale, "this test exists to pin the divergence"

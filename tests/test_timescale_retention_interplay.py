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
   ``drop_after`` IS ``event_retention_period``). Proven here: on the
   policy-armed hypertable the below-floor range is the policy's
   WHOLESALE — the sweep (outbox arm included) stays out of it since the
   retention-policy floor — and the policy run DEFEATS the carve-out at
   ~1x retention, the sweep never having had a say. The carve-out's
   positive leg (the sweep honors it on ranges it owns) is pinned by the
   floor section's vanilla/policy-less/full-range tests.
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

And the floor pins (H7-H10): TaskQ's sweeps do not pay to re-delete the
aged end the policy owns. When a ``policy_retention`` job is registered
against the table, the sweeps probe its own ``drop_after`` horizon ONCE
per run (``taskq.timescale.retention_policy_floor``) and bound their
DELETEs below-nothing at it: above the floor the POLICY owns deletion
(silent, chunk-granular, watermark-blind); below it the sweep owns
deletion (row-exact, watermark-visible). On vanilla Postgres the probe
fails open to None and the sweeps run full-range, byte-identical to the
pre-floor behavior.

Chunk-drop waits poll TimescaleDB's background worker (an external daemon
no test clock can advance); the poll is bounded and asserts on the
policy's own committed effect, mirroring the sibling module's pattern.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Generator, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._sweeps import (  # pyright: ignore[reportPrivateUsage]  # Why: the interplay tests run the REAL event-TTL sweep statement; the house pattern imports it where used.
    sweep_expired_events,
)
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
    assert row is not None, "the INSERT ... RETURNING always yields exactly one row"
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


async def test_reclaim_carveout_falls_to_chunk_policy_and_the_sweep_stays_out(
    ts_conn: asyncpg.Connection, ts_schema: str
) -> None:
    """H2: the crash-reclaim outbox carve-out is DEFEATED by the hypertable
    policy, and — since the retention-policy floor — the sweep STAYS OUT of
    the below-floor range entirely, outbox arm included.

    With the policy armed (deferred, so the drop is the test's to trigger),
    the floor makes the aged range the policy's wholesale: the sweep deletes
    NOTHING aged — the ordinary event because it is past retention and below
    the floor, the outbox event because the policy's ownership defeats the
    carve-out below the floor anyway (an aged lock_expired event's chunk
    drops at plain retention whether or not the sweep would have kept it to
    100x). Then ONE real policy run drops the aged chunk outright. The
    carve-out's positive leg — the sweep HONORS it on a range it owns — is
    pinned full-range by H10a (vanilla) and H10b (policy-less hypertable),
    and inside the window by H8's inside-row proof.
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
    assert deleted == 0, (
        "the aged range is the policy's (the floor is the policy's own "
        "drop_after): the sweep does not pay to re-delete it, outbox arm "
        "included — the policy's chunk drops defeat the carve-out below the "
        "floor regardless"
    )
    surviving = {
        int(r["id"]) for r in await ts_conn.fetch(f"SELECT id FROM {ts_schema}.job_events")
    }
    assert aged_outbox_id in surviving, (
        "the aged lock_expired event survives the sweep: below the floor it "
        "is the policy's, and no sweep arm deletes it first"
    )
    assert aged_ordinary_id in surviving
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
    """H5: the sweeps' SQL is mode-agnostic — the same script produces
    IDENTICAL observable outcomes on vanilla Postgres and on the hypertable
    mode.

    Run at the FULL-RANGE wiring (the floor probe patched to its always-
    None counterfactual on both engines), because that is the claim this
    hypothesis owns: the sweep statements themselves carry no
    mode-conditional behavior — same counts, same survivors, same
    watermark on both engines. With the floor LIVE on a policy-armed
    hypertable, the aged range is the policy's and the outcomes
    legitimately diverge (the sweeps skip what the policy will drop);
    that composition's end-state parity is H9's proof, and the floor's
    red/green is H7/H8.
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
            # The full-range wiring on both engines: pin the SQL's
            # mode-agnosticism, not the floor's composition (H9 owns that).
            with _no_floor_patch():
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


# ── H7-H10: the policy floor — the sweeps stop paying for the policy's range


async def _no_floor(
    conn: asyncpg.Connection,
    schema: str,
    table: str,
    partition_col: str,
    now: datetime | None = None,
) -> None:
    """The floor probe's counterfactual twin: always None (pre-fix behavior)."""
    return None


@contextmanager
def _no_floor_patch() -> Generator[None, None, None]:
    """Patch BOTH sweep modules' floor probe to the always-None
    counterfactual for the enclosed block, restoring both bindings after.

    The two sweeps import the probe by name into their own modules
    (``taskq.backend._sweeps`` and ``taskq.worker._leader_shared``), so
    the counterfactual must patch both bindings or one sweep would still
    see the real floor.
    """
    from taskq.backend import _sweeps as pg_sweeps
    from taskq.worker import _leader_shared

    originals = (pg_sweeps.retention_policy_floor, _leader_shared.retention_policy_floor)  # pyright: ignore[reportPrivateImportUsage]  # Why: the sweeps bind the probe into their own modules by name; the counterfactual must patch both bindings.
    pg_sweeps.retention_policy_floor = _no_floor  # type: ignore[assignment]  # pyright: ignore[reportPrivateImportUsage]
    _leader_shared.retention_policy_floor = _no_floor  # type: ignore[assignment]  # pyright: ignore[reportPrivateImportUsage]
    try:
        yield
    finally:
        pg_sweeps.retention_policy_floor = originals[0]  # type: ignore[assignment]  # pyright: ignore[reportPrivateImportUsage]
        _leader_shared.retention_policy_floor = originals[1]  # type: ignore[assignment]  # pyright: ignore[reportPrivateImportUsage]


async def test_policy_floor_keeps_the_expiry_sweep_out_of_the_policy_range(
    ts_conn: asyncpg.Connection, ts_schema: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """H7, RED/GREEN: with the archive policy armed, the expiry sweep's
    deleted-count over the BELOW-FLOOR range is zero — and the counterfactual
    (floor off, the pre-fix wiring) shows the sweep paying to delete exactly
    those rows.

    Three bands, all with ``expire_at`` in the past except the fresh one:

    * aged (``finished_at`` 3.5d, below the 2-day floor): policy-owned.
      RED deletes them (4 total); GREEN deletes none of them.
    * inside the window (``finished_at`` 1d, above the floor): the
      sweep's row-exact work below the policy's horizon — deleted in
      BOTH runs. The floor never touches the young range.
    * fresh (``expire_at`` future): never eligible, survives both.

    Then one real policy run drops the aged chunk: the rows GREEN left
    leave through the policy's own path. End state: identical, different
    work distribution.
    """
    from taskq.timescale import retention_policy_floor
    from taskq.worker import _leader_shared

    now = datetime.now(UTC)
    # The floor IS the policy's own horizon (its registered drop_after =
    # _TEST_ARCHIVE_RETENTION), not a re-derivation.
    floor = await retention_policy_floor(ts_conn, ts_schema, "jobs_archive", "finished_at", now=now)
    assert floor == now - _TEST_ARCHIVE_RETENTION

    def _seed(*, finished_at: datetime, expire_at: datetime) -> Any:
        return _seed_archive_row(ts_conn, ts_schema, finished_at=finished_at, expire_at=expire_at)

    aged_at = now - _AGED_ARCHIVE_FINISHED_AT
    expired_at = now - timedelta(hours=1)
    # ── RED: the floor off — the pre-fix sweep deletes the aged rows too.
    aged_red = [await _seed(finished_at=aged_at, expire_at=expired_at) for _ in range(3)]
    inside_red = await _seed(finished_at=now - timedelta(days=1), expire_at=expired_at)
    monkeypatch.setattr(_leader_shared, "retention_policy_floor", _no_floor)
    try:
        pre_fix = await archive_expiry_sweep(ts_conn, schema=ts_schema, batch_size=100)
    finally:
        monkeypatch.undo()
    assert pre_fix.total_deleted == 4, (
        "the counterfactual: without the floor the sweep pays to delete the "
        "aged (policy-owned) rows row by row"
    )

    # ── GREEN: the real floor — the sweep deletes exactly the inside-window
    # row and stays out of the policy's range.
    aged_green = [await _seed(finished_at=aged_at, expire_at=expired_at) for _ in range(3)]
    inside_green = await _seed(finished_at=now - timedelta(days=1), expire_at=expired_at)
    green = await archive_expiry_sweep(ts_conn, schema=ts_schema, batch_size=100)
    assert green.total_deleted == 1, (
        "the sweep owns exactly the window: the inside-row deletes row-exact, "
        "the below-floor rows are left to the policy"
    )
    surviving = {r["id"] for r in await ts_conn.fetch(f"SELECT id FROM {ts_schema}.jobs_archive")}
    assert set(aged_green).issubset(surviving), (
        "the below-floor rows survive the sweep: the policy owns their deletion"
    )
    assert inside_green not in surviving
    assert set(aged_red) & surviving == set()
    assert inside_red not in surviving

    # The policy's own run closes the loop: the below-floor rows leave
    # through the chunk drop, at chunk granularity.
    async def _aged_green_gone() -> bool:
        ids = {r["id"] for r in await ts_conn.fetch(f"SELECT id FROM {ts_schema}.jobs_archive")}
        return not (set(aged_green) & ids)

    await _force_policies_now(ts_conn, ts_schema)
    await _wait_for(
        _aged_green_gone,
        what="the policy to drop the below-floor chunk the sweep skipped",
    )


async def test_policy_floor_bounds_the_event_ttl_sweep_and_the_window_stays_exact(
    ts_conn: asyncpg.Connection, ts_schema: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """H8, RED/GREEN on the event TTL sweep, with the floor read from a
    policy whose ``drop_after`` (3 days) is deliberately LARGER than the
    sweep's retention (1 day): the floor must come from the policy's
    registered config, not from the sweep's retention argument —
    otherwise the inside-window proof below is vacuous (the two bounds
    would coincide and the window would be empty).

    The outbox arm carries the floor too: below it the policy's chunk
    drop defeats the carve-out anyway (H2), so the sweep does not pay to
    re-delete what the policy owns, and above it the carve-out keeps its
    full strength (pinned by the H2 test). The inside-window deletion
    still advances the watermark — below the floor the sweep deletes
    nothing, so the watermark stays put exactly as H4b pinned for the
    policy's own drops.
    """
    from taskq.backend import _sweeps as pg_sweeps
    from taskq.timescale import retention_policy_floor

    widened = timedelta(days=3)
    await ts_conn.execute(
        "SELECT remove_retention_policy($1::regclass, if_exists => TRUE)",
        f'"{ts_schema}"."job_events"',
    )
    await ts_conn.execute(
        "SELECT add_retention_policy($1::regclass, $2::interval, if_not_exists => TRUE)",
        f'"{ts_schema}"."job_events"',
        widened,
    )
    now = datetime.now(UTC)
    floor = await retention_policy_floor(ts_conn, ts_schema, "job_events", "occurred_at", now=now)
    assert floor == now - widened, (
        "the floor is the policy's OWN horizon (drop_after from the "
        "registered config), never a re-derivation from the sweep's retention"
    )

    below_at = now - timedelta(days=4.5)  # past retention AND below the 3d floor
    # (the 4.5d-old event's chunk ENDS 4d ago — a full day strictly below
    # the drop boundary, the half-chunk margin rule the module header pins;
    # 3.5d would put the chunk's end exactly ON the boundary)
    inside_at = now - timedelta(days=2)  # past retention, inside the window
    below_red = await _seed_event(ts_conn, ts_schema, occurred_at=below_at, detail={})
    inside_red = await _seed_event(ts_conn, ts_schema, occurred_at=inside_at, detail={})

    # ── RED: the floor off — the pre-fix sweep deletes the below-floor
    # event too.
    monkeypatch.setattr(pg_sweeps, "retention_policy_floor", _no_floor)
    try:
        pre_fix = await sweep_expired_events(
            ts_conn, schema=ts_schema, retention=_TEST_EVENT_RETENTION, batch_size=100
        )
    finally:
        monkeypatch.undo()
    assert pre_fix == 2, "the counterfactual: the pre-fix sweep re-deletes the policy's range"

    # ── GREEN: the real floor — exactly the inside-window event deletes,
    # row-exact, watermark-advancing.
    below_green = await _seed_event(ts_conn, ts_schema, occurred_at=below_at, detail={})
    inside_green = await _seed_event(ts_conn, ts_schema, occurred_at=inside_at, detail={})
    fresh_id = await _seed_event(ts_conn, ts_schema, occurred_at=now, detail={})
    deleted = await sweep_expired_events(
        ts_conn, schema=ts_schema, retention=_TEST_EVENT_RETENTION, batch_size=100
    )
    assert deleted == 1, "the sweep owns exactly the inside-window row"
    surviving = {
        int(r["id"]) for r in await ts_conn.fetch(f"SELECT id FROM {ts_schema}.job_events")
    }
    assert below_green in surviving, "the below-floor event is left to the policy"
    assert below_red not in surviving and inside_red not in surviving
    assert inside_green not in surviving and fresh_id in surviving
    watermark = await _event_watermark(ts_conn, ts_schema)
    assert watermark >= inside_green, (
        "the inside-window deletion advances the watermark: below the floor "
        "the sweep deletes nothing, so the watermark moves only over rows it "
        "actually deleted"
    )

    # The policy closes the loop over the row it now owns.
    async def _below_green_gone() -> bool:
        ids = {int(r["id"]) for r in await ts_conn.fetch(f"SELECT id FROM {ts_schema}.job_events")}
        return below_green not in ids

    await _force_policies_now(ts_conn, ts_schema)
    await _wait_for(
        _below_green_gone,
        what="the policy to drop the below-floor event's chunk",
    )


async def test_policy_run_plus_floor_composes_to_todays_end_state(timescale_dsn: str) -> None:
    """H9, parity: policy-run + sweeps compose to the SAME end state with
    the floor as without it — only the work distribution changes.

    One seed script (aged+expired archive rows, aged ordinary events, an
    aged outbox event, fresh survivors of each), run twice on two fresh
    hypertable schemas:

    * no-floor (today's pre-fix behavior): the sweeps row-delete the aged
      range; the policy run then drops the aged chunks the sweeps
      emptied (the outbox row's chunk included — H2).
    * floor (the fix): the sweeps skip the below-floor range; the policy
      run drops the aged chunks with the rows still in them.

    The surviving ROW SETS are identical across the board. The one
    bookkeeping row that legitimately differs is the event-prune
    watermark: the no-floor scenario advanced it by deleting
    (watermark-visible), the floor scenario's aged deletion went through
    the policy, which advances nothing — H4b's pinned gap, not a new one.
    """

    async def run_scenario(*, use_floor: bool) -> dict[str, Any]:
        schema = f"tsr_floor_parity_{str(use_floor).lower()}_{new_uuid().hex[:8]}"
        conn = await asyncpg.connect(timescale_dsn)
        try:
            await _migrate(conn, schema)
            await enable_hypertables(
                conn, schema=schema, settings=_ts_settings(timescale_dsn, schema)
            )
            await _schedule_policies(
                conn, schema, next_start=datetime.now(UTC) + timedelta(days=3650)
            )

            now = datetime.now(UTC)
            expired_at = now - timedelta(hours=1)
            aged_archive = [
                await _seed_archive_row(
                    conn, schema, finished_at=now - _AGED_ARCHIVE_FINISHED_AT, expire_at=expired_at
                )
                for _ in range(3)
            ]
            fresh_archive = await _seed_archive_row(
                conn, schema, finished_at=now, expire_at=now + timedelta(days=365)
            )
            aged_ordinary = [
                await _seed_event(conn, schema, occurred_at=now - _AGED_EVENT_OCCURRENCE, detail={})
                for _ in range(2)
            ]
            aged_outbox = await _seed_event(
                conn,
                schema,
                occurred_at=now - _AGED_EVENT_OCCURRENCE,
                detail={"reason": "lock_expired"},
            )
            fresh_event = await _seed_event(conn, schema, occurred_at=now, detail={})

            async def _run_both_sweeps() -> None:
                await archive_expiry_sweep(conn, schema=schema, batch_size=100)
                await sweep_expired_events(
                    conn, schema=schema, retention=_TEST_EVENT_RETENTION, batch_size=100
                )

            if use_floor:
                await _run_both_sweeps()
            else:
                # The pre-fix wiring: both sweeps probe through a stub that
                # always answers None. Scoped patch, undone before the
                # policy run so the real policies drive the drop.
                with _no_floor_patch():
                    await _run_both_sweeps()

            # The policy run closes the aged end in BOTH scenarios: in the
            # floor scenario it does the deleting (chunk drops), in the
            # no-floor scenario the sweeps already did it row-exactly and
            # the run finds aged chunks holding only the outbox row —
            # which it drops too (H2).
            await _force_policies_now(conn, schema)

            async def _aged_gone() -> bool:
                archive_ids = {
                    r["id"] for r in await conn.fetch(f"SELECT id FROM {schema}.jobs_archive")
                }
                event_ids = {
                    int(r["id"]) for r in await conn.fetch(f"SELECT id FROM {schema}.job_events")
                }
                return not (
                    set(aged_archive) & archive_ids
                    or (set(aged_ordinary) | {aged_outbox}) & event_ids
                )

            await _wait_for(_aged_gone, what="the policy to drop the aged chunks")

            archive_ids = {
                r["id"] for r in await conn.fetch(f"SELECT id FROM {schema}.jobs_archive")
            }
            event_ids = {
                int(r["id"]) for r in await conn.fetch(f"SELECT id FROM {schema}.job_events")
            }
            return {
                "aged_archive_alive": sorted(set(aged_archive) & archive_ids),
                "fresh_archive_alive": fresh_archive in archive_ids,
                "aged_ordinary_alive": sorted(set(aged_ordinary) & event_ids),
                "aged_outbox_alive": aged_outbox in event_ids,
                "fresh_event_alive": fresh_event in event_ids,
                "watermark": await _event_watermark(conn, schema),
            }
        finally:
            await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            await conn.close()

    vanilla_outcome = await run_scenario(use_floor=False)
    floor_outcome = await run_scenario(use_floor=True)

    for key in (
        "aged_archive_alive",
        "fresh_archive_alive",
        "aged_ordinary_alive",
        "aged_outbox_alive",
        "fresh_event_alive",
    ):
        assert vanilla_outcome[key] == floor_outcome[key], (
            f"end-state drift on {key}:\nno-floor={vanilla_outcome[key]}\nfloor={floor_outcome[key]}"
        )
    # The surviving data is identical; the watermark is the one pinned
    # difference (H4b's gap, inherited — the policy never advances it).
    assert vanilla_outcome["watermark"] > floor_outcome["watermark"], (
        "the no-floor scenario advanced the watermark by deleting; the floor "
        "scenario's aged deletion went through the watermark-blind policy"
    )


async def test_vanilla_postgres_probe_fails_open_and_sweeps_run_full_range(
    pg_dsn: str,
) -> None:
    """H10a, the regression pin: on vanilla Postgres the floor probe fails
    open to None (the ``timescaledb_information`` views do not exist — the
    probe's very first execution raises UndefinedTable and the probe
    answers None, logging at debug, never raising) and both sweeps run
    FULL-RANGE, byte-identical to the pre-floor behavior: aged rows
    delete, fresh rows survive, counts exact.

    This is the no-floor contract the whole fix rests on: every non-
    hypertable deployment's sweeps must be untouched.
    """
    from taskq.timescale import retention_policy_floor

    schema = "tsr_no_floor_" + new_uuid().hex[:12]
    conn = await asyncpg.connect(pg_dsn)
    try:
        await _migrate(conn, schema)
        assert await retention_policy_floor(conn, schema, "job_events", "occurred_at") is None
        assert await retention_policy_floor(conn, schema, "jobs_archive", "finished_at") is None
        # A table that does not exist at all: same fail-open.
        assert await retention_policy_floor(conn, schema, "no_such_table", "x") is None

        now = datetime.now(UTC)
        expired_at = now - timedelta(hours=1)
        aged_archive = [
            await _seed_archive_row(
                conn, schema, finished_at=now - _AGED_ARCHIVE_FINISHED_AT, expire_at=expired_at
            )
            for _ in range(3)
        ]
        fresh_archive = await _seed_archive_row(
            conn, schema, finished_at=now, expire_at=now + timedelta(days=365)
        )
        aged_event = await _seed_event(
            conn, schema, occurred_at=now - _AGED_EVENT_OCCURRENCE, detail={}
        )
        aged_outbox = await _seed_event(
            conn,
            schema,
            occurred_at=now - _AGED_EVENT_OCCURRENCE,
            detail={"reason": "lock_expired"},
        )
        fresh_event = await _seed_event(conn, schema, occurred_at=now, detail={})

        expiry = await archive_expiry_sweep(conn, schema=schema, batch_size=100)
        assert expiry.total_deleted == 3, "full-range: the aged rows delete exactly as before"
        deleted = await sweep_expired_events(
            conn, schema=schema, retention=_TEST_EVENT_RETENTION, batch_size=100
        )
        assert deleted == 1, "full-range: the aged ordinary event deletes, the carve-out holds"

        archive_ids = {r["id"] for r in await conn.fetch(f"SELECT id FROM {schema}.jobs_archive")}
        event_ids = {int(r["id"]) for r in await conn.fetch(f"SELECT id FROM {schema}.job_events")}
        assert archive_ids == {fresh_archive}
        assert event_ids == {aged_outbox, fresh_event}
        assert aged_archive and aged_event not in event_ids
    finally:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


async def test_hypertable_without_a_policy_has_no_floor_and_the_sweep_stays_full_range(
    ts_conn: asyncpg.Connection, ts_schema: str
) -> None:
    """H10b, the probe's edge: a hypertable WITHOUT a registered policy
    answers None — nothing owns the aged end, so the sweep must run
    full-range (the H1 no-op only holds because a policy IS armed there).

    Also pins the probe's other None paths on the real engine: a table
    that is not a hypertable at all (``jobs`` — it is never converted),
    and a partition-column name that does not match the hypertable's time
    dimension (the floor's clock would be the wrong clock). Then the
    policy is removed and the aged rows delete through the sweep, the
    carve-out at full strength again.
    """
    from taskq.timescale import retention_policy_floor

    now = datetime.now(UTC)
    # Armed: the floor is present and is EXACTLY the policy's horizon.
    floor = await retention_policy_floor(ts_conn, ts_schema, "job_events", "occurred_at", now=now)
    assert floor == now - _TEST_EVENT_RETENTION
    # Not a hypertable: no floor.
    assert await retention_policy_floor(ts_conn, ts_schema, "jobs", "finished_at", now=now) is None
    # Wrong partition column: the floor's clock would be the wrong clock.
    assert (
        await retention_policy_floor(ts_conn, ts_schema, "job_events", "finished_at", now=now)
        is None
    )

    # Policy removed: nothing owns the aged end any more.
    await ts_conn.execute(
        "SELECT remove_retention_policy($1::regclass, if_exists => TRUE)",
        f'"{ts_schema}"."job_events"',
    )
    assert (
        await retention_policy_floor(ts_conn, ts_schema, "job_events", "occurred_at", now=now)
        is None
    )

    aged_id = await _seed_event(
        ts_conn, ts_schema, occurred_at=now - _AGED_EVENT_OCCURRENCE, detail={}
    )
    outbox_id = await _seed_event(
        ts_conn,
        ts_schema,
        occurred_at=now - _AGED_EVENT_OCCURRENCE,
        detail={"reason": "lock_expired"},
    )
    fresh_id = await _seed_event(ts_conn, ts_schema, occurred_at=now, detail={})

    deleted = await sweep_expired_events(
        ts_conn, schema=ts_schema, retention=_TEST_EVENT_RETENTION, batch_size=100
    )
    assert deleted == 1, "full-range on the policy-less hypertable: the aged ordinary event deletes"
    surviving = {
        int(r["id"]) for r in await ts_conn.fetch(f"SELECT id FROM {ts_schema}.job_events")
    }
    assert surviving == {outbox_id, fresh_id}, (
        "the carve-out holds at full strength when nothing owns the aged end"
    )
    assert aged_id not in surviving

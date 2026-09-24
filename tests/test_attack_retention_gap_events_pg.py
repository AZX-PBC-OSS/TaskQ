# ruff: noqa: S608  # Why: schema names are generated/validated identifiers, every value is $-bound.
"""ATTACK pins (real PG): both retention deleters leave the watermark a
trailing-watermark consumer can test its cursor against.

The loss class these pins close: a ``watch_reclaims`` consumer parked at
cursor K while event retention deletes ``(K, K+n]`` used to resume into a
silent skip to live - the surviving rows returned, no error anywhere, the
consumer free to believe it saw everything. The fix is the event-prune
watermark (``job_events_prune_state``, migration 01.00.20_01): each deleter
advances ``pruned_through_id`` to the highest event id its statement deleted,
in the same transaction as the delete, and the stream gate turns a cursor
strictly below that bound into ``EventRetentionGapError``.

What is pinned here, on a real Postgres:

* the event-retention sweep's outbox age-cap arm (the arm that deletes the
  crash-reclaim slice the ordinary window carves out) advances the watermark
  to the highest id it deleted, monotonically across ticks, write-free when
  it deletes nothing;
* the carve-out itself survives just-lapsed retention and never advances the
  watermark over rows it kept;
* the terminal prune's cascade (``DELETE FROM jobs`` removing the archived
  jobs' events via the FK) advances the same bound through its own
  ``event_watermark`` CTE - the second deleter, the one a watermark on the
  sweep alone would miss;
* the backend's watermark read answers the committed bound, and the stream
  gate raises :class:`EventRetentionGapError` on a resumed cursor behind it
  and passes a cursor at or above it.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend.clock import SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.constants import (
    DEFAULT_EVENT_WRITER_BATCH_SIZE,
    DEFAULT_EVENT_WRITER_STATEMENT_TIMEOUT_MS,
    MAX_RESULT_BYTES,
    RECLAIM_OUTBOX_RETENTION_MULTIPLIER,
)
from taskq.exceptions import EventRetentionGapError
from taskq.testing.fixtures import ModulePgSchema
from taskq.worker.leader import prune_terminal_jobs

pytestmark = pytest.mark.integration

_RETENTION = timedelta(days=1)
#: The outbox arm's age: far past RETENTION x MULTIPLIER, so the sweep's
#: carve-out has lapsed for these rows and the age-cap arm deletes them.
_OUTBOX_AGE = _RETENTION * RECLAIM_OUTBOX_RETENTION_MULTIPLIER * 2


class _TestBackendSettings:
    """The declared ``BackendSettings`` protocol's fields (mirrors
    test_admin_audit_trail.py's double)."""

    schema_name: str
    dispatch_oversample: int = 2
    dispatcher_command_timeout: float = 5.0
    result_max_bytes: int = MAX_RESULT_BYTES
    event_writer_batch_size: int = DEFAULT_EVENT_WRITER_BATCH_SIZE
    event_writer_statement_timeout_ms: float = DEFAULT_EVENT_WRITER_STATEMENT_TIMEOUT_MS
    event_writer_reduced_batch_divisor: int = 4
    sweep_breaker_failure_threshold: int = 3
    sweep_breaker_window_secs: float = 600.0
    max_pending_lock_timeout_ms: float = 5000.0
    unique_for_lock_timeout_ms: float = 5000.0
    idempotency_lock_timeout_ms: float = 5000.0
    max_retry_backoff: timedelta = timedelta(seconds=60)

    def __init__(self, schema_name: str) -> None:
        self.schema_name = schema_name


class _TestBackendDeps:
    settings: _TestBackendSettings
    worker_pool: asyncpg.Pool
    heartbeat_pool: asyncpg.Pool
    dispatcher_pool: asyncpg.Pool | None = None

    def __init__(self, schema: str, pool: asyncpg.Pool) -> None:
        self.settings = _TestBackendSettings(schema)
        self.worker_pool = pool
        self.heartbeat_pool = pool


@pytest.fixture()
def make_backend(module_pg_pool: asyncpg.Pool, module_pg_schema: ModulePgSchema) -> Any:
    def _make() -> PostgresBackend:
        return PostgresBackend(
            _TestBackendDeps(module_pg_schema.schema_name, module_pg_pool),  # pyright: ignore[reportArgumentType]  # Why: the protocol double mirrors test_admin_audit_trail.py.
            clock=SystemClock(),
            cancellation_grace_period=timedelta(seconds=5),
            cleanup_grace_period=timedelta(seconds=5),
        )

    return _make


async def _seed_job(
    conn: asyncpg.Connection,
    schema: str,
    *,
    status: str = "running",
) -> UUID:
    jid = new_uuid()
    await conn.execute(
        f"""INSERT INTO "{schema}".jobs (id, actor, queue, payload, status, max_attempts, retry_kind)
        VALUES ($1, 'gap_actor', 'default', '{{"v":1}}'::jsonb, '{status}', 3, 'transient')""",
        jid,
    )
    return jid


async def _seed_reclaim_event(
    conn: asyncpg.Connection,
    schema: str,
    job_id: UUID,
    *,
    age: timedelta,
) -> int:
    """One crash-reclaim outbox event (the row poll_reclaim_events filters
    for) at *age* old. Returns its id."""
    return int(
        await conn.fetchval(
            f"""INSERT INTO "{schema}".job_events (job_id, occurred_at, kind, detail)
            VALUES ($1, clock_timestamp() - $2::interval, 'state_change',
                    jsonb_build_object('reason', 'lock_expired'))
            RETURNING id""",
            job_id,
            age,
        )
    )


class _GateClient:
    """The getattr-probe surface ``_assert_no_retention_gap`` needs: only
    the watermark capability matters, the gate reads nothing else."""

    def __init__(self, backend: PostgresBackend) -> None:
        self.backend = backend


class TestSweepAdvancesWatermark:
    async def test_outbox_age_cap_arm_advances_watermark_monotonically(
        self,
        module_pg_schema: ModulePgSchema,
        clean_pg_conn: asyncpg.Connection,
        make_backend: Any,
    ) -> None:
        """The consumer parked at e1 while the outbox arm deletes
        (e1, e3]: the backend poll answers zero rows with no error (the old
        silent loss at the poll layer), the watermark reads e3, and the
        stream gate turns the resumed cursor into EventRetentionGapError
        naming both numbers."""
        schema = module_pg_schema.schema_name
        job = await _seed_job(clean_pg_conn, schema)
        e1 = await _seed_reclaim_event(clean_pg_conn, schema, job, age=_OUTBOX_AGE)
        e2 = await _seed_reclaim_event(clean_pg_conn, schema, job, age=_OUTBOX_AGE)
        e3 = await _seed_reclaim_event(clean_pg_conn, schema, job, age=_OUTBOX_AGE)
        assert e1 < e2 < e3

        backend = make_backend()

        # Consumer parked at e1: only e1 delivered.
        count = await backend.sweep_expired_events(
            clean_pg_conn, schema=schema, retention=_RETENTION
        )
        assert count == 3, "the age-capped outbox arm must delete all three rows"

        # The old silent behavior at the backend layer: the poll answers
        # zero rows and no error. The watermark is what makes the STREAM
        # loud about why.
        assert await backend.poll_reclaim_events(e1) == []
        assert await backend.event_prune_watermark() == e3, (
            "the watermark must name the highest id the sweep deleted"
        )

        from taskq.client._taskq import _assert_no_retention_gap

        with pytest.raises(EventRetentionGapError) as exc_info:
            await _assert_no_retention_gap(_GateClient(backend), e1)
        assert exc_info.value.cursor == e1
        assert exc_info.value.pruned_through_id == e3

        # A caught-up cursor (the watermark itself) passes the gate: every
        # deleted id was at or below a position already delivered.
        await _assert_no_retention_gap(_GateClient(backend), e3)

    async def test_drained_tick_writes_nothing_and_bound_never_regresses(
        self,
        module_pg_schema: ModulePgSchema,
        clean_pg_conn: asyncpg.Connection,
        make_backend: Any,
    ) -> None:
        """A second sweep with nothing eligible must not move (or rewrite)
        the watermark, the monotonicity the concurrent-duplicate-sweep
        interleaving relies on."""
        schema = module_pg_schema.schema_name
        job = await _seed_job(clean_pg_conn, schema)
        await _seed_reclaim_event(clean_pg_conn, schema, job, age=_OUTBOX_AGE)

        backend = make_backend()
        count = await backend.sweep_expired_events(
            clean_pg_conn, schema=schema, retention=_RETENTION
        )
        assert count == 1
        first = await backend.event_prune_watermark()
        assert first > 0

        count = await backend.sweep_expired_events(
            clean_pg_conn, schema=schema, retention=_RETENTION
        )
        assert count == 0
        assert await backend.event_prune_watermark() == first, (
            "a drained tick must not rewrite the bound: GREATEST keeps a "
            "concurrent duplicate sweep from moving it backwards"
        )

    async def test_carve_out_keeps_young_outbox_rows_and_never_advances_the_watermark(
        self,
        module_pg_schema: ModulePgSchema,
        clean_pg_conn: asyncpg.Connection,
        make_backend: Any,
    ) -> None:
        """The crash-reclaim carve-out survives just-lapsed retention (the
        pinned behavior): young outbox rows are kept, the watermark does
        not advance over rows the sweep kept, and no cursor raises."""
        schema = module_pg_schema.schema_name
        job = await _seed_job(clean_pg_conn, schema)
        # Just-past ordinary retention, far inside the outbox cap: kept.
        e1 = await _seed_reclaim_event(clean_pg_conn, schema, job, age=_RETENTION * 2)

        backend = make_backend()
        count = await backend.sweep_expired_events(
            clean_pg_conn, schema=schema, retention=_RETENTION
        )
        assert count == 0
        assert await backend.event_prune_watermark() == 0

        from taskq.client._taskq import _assert_no_retention_gap

        # A cursor at the event - or below it - is safe: nothing was deleted.
        await _assert_no_retention_gap(_GateClient(backend), e1)
        await _assert_no_retention_gap(_GateClient(backend), 0)

        # And the row the carve-out kept is still deliverable.
        rows = await backend.poll_reclaim_events(0)
        assert [r.event_id for r in rows] == [e1]


class TestPruneCascadeAdvancesWatermark:
    async def test_prune_cascade_advances_watermark(
        self,
        module_pg_schema: ModulePgSchema,
        clean_pg_conn: asyncpg.Connection,
        make_backend: Any,
    ) -> None:
        """The terminal prune deletes jobs; the FK cascade deletes their
        events. A consumer parked below those events' ids must find the
        bound advanced by the prune's own event_watermark CTE, not just by
        the retention sweep: the cascade is a second deleter and a
        sweep-only watermark would leave its losses silent."""
        schema = module_pg_schema.schema_name
        job = await _seed_job(clean_pg_conn, schema, status="succeeded")
        # A terminal-aged job: prune retention 1 day, finished 30 days ago.
        await clean_pg_conn.execute(
            f"UPDATE \"{schema}\".jobs SET finished_at = clock_timestamp() - interval '30 days'"
            " WHERE id = $1",
            job,
        )
        # Two reclaim events: the consumer delivered e1 (cursor e1), the
        # prune cascades both away, the watermark lands on e2 and the
        # cursor sits strictly behind it. (A single event would make the
        # natural test cursor e1-1, which collides with the fresh-watcher
        # sentinel 0 on a fresh schema's first id.)
        e1 = await _seed_reclaim_event(clean_pg_conn, schema, job, age=_OUTBOX_AGE)
        e2 = await _seed_reclaim_event(clean_pg_conn, schema, job, age=_OUTBOX_AGE)
        assert e1 < e2

        backend = make_backend()

        # No sweep has run; the watermark is still 0 before the prune.
        assert await backend.event_prune_watermark() == 0

        result = await prune_terminal_jobs(
            clean_pg_conn,
            retention_per_status={"succeeded": timedelta(days=1)},
            archive_retention=timedelta(days=365),
            batch_size=100,
            schema=schema,
        )
        assert result.total_deleted == 1

        # The job is gone (archived), the event cascaded away with it,
        # and the watermark names the loss.
        live = await clean_pg_conn.fetchval(
            f'SELECT count(*) FROM "{schema}".jobs WHERE id = $1', job
        )
        assert live == 0
        archived = await clean_pg_conn.fetchval(
            f'SELECT count(*) FROM "{schema}".jobs_archive WHERE id = $1', job
        )
        assert archived == 1
        events = await clean_pg_conn.fetchval(
            f'SELECT count(*) FROM "{schema}".job_events WHERE id = ANY($1::bigint[])', [e1, e2]
        )
        assert events == 0, "the FK cascade must have removed the archived job's events"

        watermark = await backend.event_prune_watermark()
        assert watermark == e2, (
            "the prune's cascade is a retention deleter: it must advance the "
            "same bound the sweep does, or its losses stay silent"
        )

        from taskq.client._taskq import _assert_no_retention_gap

        with pytest.raises(EventRetentionGapError) as exc_info:
            await _assert_no_retention_gap(_GateClient(backend), e1)
        assert exc_info.value.cursor == e1
        assert exc_info.value.pruned_through_id == e2
        # At the watermark: safe.
        await _assert_no_retention_gap(_GateClient(backend), e2)

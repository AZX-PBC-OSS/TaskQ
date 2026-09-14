# ruff: noqa: S608  # Why: schema is a fixed test identifier, not user input; every value is $-bound.
"""Red-team storm: the reclaim-event outbox under mass failure — fill
vs drain geometry.

Model (from ``backend/_sweeps.py`` sweep 1, ``backend/_reads.py``
``_poll_reclaim_events``, ``client/_taskq.py`` ``_watch_reclaims_pg``):

* FILL: one ``reclaim_expired_locks`` call writes at most
  ``event_writer_batch_size`` (default 100) ``lock_expired`` events in
  one short transaction — the bounded-writes class already pinned by
  ``test_sweepaudit_bounded_writes`` / ``test_sweep_expired_locks_bounded``
  (not re-pinned here; this file measures the RATIO side).
* DRAIN: ``poll_reclaim_events`` returns at most ``limit`` rows per call
  (default 100, ``DEFAULT_RECLAIM_POLL_LIMIT``; ``watch_reclaims`` uses
  ``_WATCH_RECLAIMS_BATCH_LIMIT`` = 100), and the consumer re-polls
  IMMEDIATELY after a full batch — so a crash storm larger than one
  batch drains at full poll speed, not one batch per ``poll_timeout``.
* RATIO: at the default knobs the per-call fill bound and the per-call
  drain bound are the SAME 100 — one committed sweep batch is exactly
  one drainable poll batch, so a tight-loop consumer cannot fall behind
  per call. Backlog accrues only when the consumer's downstream (the
  ``yield`` into the caller's handler) is slower than the fill rate;
  the outbox slice is exempt from event retention at every age
  (immortal-until-consumed — pinned by
  ``test_rt_orphans_outbox_immortal_events``, not duplicated here).

DESIGN ASK (dispositioned, not red): nothing bounds or alarms on outbox
DEPTH. The default-on risk probe
(``check_reclaim_visibility_delay_risk``, run by every ``watch_reclaims``
consumer every 60 s) inspects long-running ``job_events`` *writer
transactions*, not queue depth — a consumer stalled for an hour with a
growing backlog produces no signal anywhere. A depth alarm would be new
design (a gauge on the unconsumed ``lock_expired`` slice or a
fill/drain-rate counter); this file pins the per-call bounds the alarm
would be built on.

The 2 s ``RECLAIM_EVENT_VISIBILITY_DELAY`` trailing watermark is the
consumer-side latency damper: freshly-committed storm events are
withheld for the margin so a poll can never skip a lower-id row that is
still uncommitted — pinned below from the observable side.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_base62, new_uuid
from taskq.backend.clock import SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.constants import DEFAULT_RECLAIM_POLL_LIMIT
from taskq.migrate import apply_pending
from taskq.settings import WorkerSettings
from taskq.testing.settings import make_integration_settings

pytestmark = pytest.mark.integration

_ACTOR = "storm_outbox_actor"
_QUEUE = "default"

# 250 = 2 full 100-row sweep batches + a 50-row remainder: enough to
# observe the multi-batch fill AND the multi-poll drain with their exact
# per-call bounds, small enough to stay fast.
_STORM_N = 250
_BATCH = 100  # settings default event_writer_batch_size
_REMAINDER = 50
_POLL_LIMIT = 100


class _PoolsDeps:
    """Duck-typed ``BackendDeps``: settings plus the pools the reclaimed
    and polled paths touch (poll routes to worker_pool, reclaim to the
    notify pool → dispatcher)."""

    def __init__(self, settings: WorkerSettings, *, worker_pool: Any, dispatcher_pool: Any) -> None:
        self.settings = settings
        self.worker_pool = worker_pool
        self.heartbeat_pool = worker_pool
        self.dispatcher_pool = dispatcher_pool


async def _seed(pg_dsn: str, schema: str) -> tuple[asyncpg.Connection, list[UUID], UUID]:
    """Migrate the schema and seed one mass-crashed fleet (all locks
    expired 1 s ago, attempts remaining)."""
    admin = await asyncpg.connect(pg_dsn)
    await admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    await apply_pending(admin, schema=schema)
    await admin.execute(
        f'INSERT INTO "{schema}".actor_config (actor, queue) VALUES ($1, $2)',
        _ACTOR,
        _QUEUE,
    )
    worker_id = new_uuid()
    await admin.execute(
        f'INSERT INTO "{schema}".workers (id, hostname, pid, queues) '
        "VALUES ($1, 'storm-host', 12345, ARRAY['default'])",
        worker_id,
    )
    job_ids = [new_uuid() for _ in range(_STORM_N)]
    await admin.execute(
        f'INSERT INTO "{schema}".jobs ('
        "    id, actor, queue, payload, max_attempts, retry_kind,"
        "    status, priority, attempt, scheduled_at,"
        "    locked_by_worker, lock_expires_at, started_at, last_heartbeat_at,"
        "    cancel_phase, cancel_requested_at"
        ") SELECT"
        "    t.id, $2::text, $3::text, '{}'::jsonb,"
        "    3::smallint, 'transient',"
        "    'running', 0, 1::smallint, clock_timestamp(),"
        "    $1::uuid,"
        "    clock_timestamp() - interval '1 second',"
        "    clock_timestamp() - interval '30 seconds',"
        "    clock_timestamp() - interval '30 seconds',"
        "    0::smallint, NULL"
        " FROM unnest($4::uuid[]) AS t(id)",
        worker_id,
        _ACTOR,
        _QUEUE,
        job_ids,
    )
    return admin, job_ids, worker_id


async def _outbox_depth(admin: asyncpg.Connection, schema: str) -> int:
    return await admin.fetchval(  # type: ignore[no-any-return]
        f'SELECT count(*) FROM "{schema}".job_events '
        "WHERE kind = 'state_change' AND COALESCE(detail->>'reason', '') = 'lock_expired'"
    )


async def test_outbox_fill_and_drain_are_both_per_call_bounded_and_parity_sized(
    pg_dsn: str,
) -> None:
    """The storm's outbox geometry: fill writes ≤ 100 events per sweep
    call, drain returns ≤ 100 events per poll call, and at the default
    knobs the bounds are EQUAL — one committed sweep batch is exactly
    one drainable poll batch.

    Contract: neither side of the outbox can amplify per call — the
    sweep's batch cap and the poll's LIMIT are the paired bounds, and a
    full-batch consumer re-polls immediately (the ``watch_reclaims``
    backlog rule), so a 250-event storm fills in 3 bounded calls and
    drains in 3 bounded calls. A regression that makes either side
    per-row (or unbounded) breaks the counts below.
    """
    schema = f"tst_{new_base62()}".lower()
    settings = make_integration_settings(pg_dsn, schema_name=schema)
    admin, _job_ids, _worker = await _seed(pg_dsn, schema)
    pools: list[asyncpg.Pool] = []
    try:
        worker_pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=3)
        dispatcher_pool = await asyncpg.create_pool(
            pg_dsn, min_size=1, max_size=4, command_timeout=5.0
        )
        pools.extend([worker_pool, dispatcher_pool])
        backend = PostgresBackend(
            _PoolsDeps(settings, worker_pool=worker_pool, dispatcher_pool=dispatcher_pool),  # type: ignore[arg-type]  # Why: duck-typed BackendDeps — settings plus the pools the swept/polled paths touch.
            clock=SystemClock(),
            cancellation_grace_period=timedelta(0),
            cleanup_grace_period=timedelta(0),
        )

        # ── FILL: bounded per call, drains the storm in committed batches ──
        fill_calls: list[int] = []
        fill_per_call: list[int] = []
        for _ in range(3):
            depth_before = await _outbox_depth(admin, schema)
            reclaimed = await backend.reclaim_expired_locks(timedelta(0), timedelta(0))
            depth_after = await _outbox_depth(admin, schema)
            fill_calls.append(reclaimed)
            fill_per_call.append(depth_after - depth_before)
        assert fill_calls == [_BATCH, _BATCH, _REMAINDER], (
            f"one reclaim call must re-pend at most event_writer_batch_size "
            f"({_BATCH}) rows; got per-call reclaim counts {fill_calls} — an "
            "unbounded or per-row fill breaks the paired-bounds contract"
        )
        assert fill_per_call == [_BATCH, _BATCH, _REMAINDER], (
            f"one reclaim call must WRITE at most {_BATCH} lock_expired events; "
            f"got per-call event writes {fill_per_call}"
        )
        assert await _outbox_depth(admin, schema) == _STORM_N

        # ── DRAIN: bounded per call, ceil(N/limit) polls, immediate
        # full-batch re-poll makes the consumer keep up per call ──
        drained_batches: list[int] = []
        cursor = 0
        while True:
            events = await backend.poll_reclaim_events(
                cursor, _POLL_LIMIT, visibility_delay=timedelta(0)
            )
            if not events:
                break
            assert len(events) <= _POLL_LIMIT, (
                f"poll_reclaim_events must return at most limit "
                f"({_POLL_LIMIT}) rows per call; got {len(events)} — an "
                "unbounded drain read is the consumer-side amplification"
            )
            for evt in events:
                assert evt.event_id > cursor, (
                    "the outbox cursor must advance monotonically — a "
                    "non-advancing cursor re-delivers the storm forever"
                )
                cursor = evt.event_id
            drained_batches.append(len(events))
        assert drained_batches == [_BATCH, _BATCH, _REMAINDER], (
            f"draining {_STORM_N} events at limit {_POLL_LIMIT} must take "
            f"ceil(N/limit) = 3 bounded calls; got {drained_batches}"
        )
        assert cursor > 0, "the storm's events must actually have been delivered"

        # ── RATIO: at default knobs the fill and drain per-call bounds
        # are the SAME number — per call, drain capacity == fill rate.
        # (A depth alarm on top of this parity is the DESIGN ask in the
        # module docstring; it does not exist today.)
        assert _BATCH == _POLL_LIMIT == DEFAULT_RECLAIM_POLL_LIMIT, (
            "the fill bound (event_writer_batch_size) and the drain bound "
            "(DEFAULT_RECLAIM_POLL_LIMIT) have drifted apart — when fill "
            "exceeds drain per call, a keeping-up consumer becomes an "
            "inevitably-falling-behind one and the outbox grows without "
            "any depth signal"
        )
    finally:
        for pool in pools:
            await pool.close()
        await admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await admin.close()


async def test_visibility_delay_withholds_fresh_storm_events(pg_dsn: str) -> None:
    """The trailing watermark is the consumer-side damper: immediately
    after a reclaim batch commits, its events are younger than the 2 s
    ``RECLAIM_EVENT_VISIBILITY_DELAY`` margin and are withheld — a
    NOTIFY-woken poll cannot race past a still-uncommitted sibling
    writer and silently skip it.

    Contract: a poll at the default delay over freshly-committed storm
    events returns nothing; the same cursor at zero delay returns the
    batch. If the delay were dropped, the watermark guarantee
    (co-monotonic id/occurred_at ordering) would depend on nothing.
    """
    schema = f"tst_{new_base62()}".lower()
    settings = make_integration_settings(pg_dsn, schema_name=schema)
    admin, _job_ids, _worker = await _seed(pg_dsn, schema)
    pools: list[asyncpg.Pool] = []
    try:
        worker_pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=3)
        dispatcher_pool = await asyncpg.create_pool(
            pg_dsn, min_size=1, max_size=4, command_timeout=5.0
        )
        pools.extend([worker_pool, dispatcher_pool])
        backend = PostgresBackend(
            _PoolsDeps(settings, worker_pool=worker_pool, dispatcher_pool=dispatcher_pool),  # type: ignore[arg-type]  # Why: duck-typed BackendDeps — same shape as the fill/drain test above.
            clock=SystemClock(),
            cancellation_grace_period=timedelta(0),
            cleanup_grace_period=timedelta(0),
        )

        reclaimed = await backend.reclaim_expired_locks(timedelta(0), timedelta(0))
        assert reclaimed == _BATCH, "the first bounded batch must have committed"

        withheld = await backend.poll_reclaim_events(0, _POLL_LIMIT)
        assert withheld == [], (
            "events younger than RECLAIM_EVENT_VISIBILITY_DELAY (2 s) must "
            "be withheld by the default-delay poll — the trailing watermark "
            "is what makes a NOTIFY-driven consumer safe under a concurrent "
            "storm writer"
        )

        visible = await backend.poll_reclaim_events(0, _POLL_LIMIT, visibility_delay=timedelta(0))
        assert len(visible) == _BATCH, (
            "the same cursor at zero delay must return the committed batch — "
            "proving the empty default-delay poll was the watermark "
            "withholding, not a lost write"
        )
    finally:
        for pool in pools:
            await pool.close()
        await admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await admin.close()

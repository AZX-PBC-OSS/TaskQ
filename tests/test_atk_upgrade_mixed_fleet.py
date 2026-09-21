# ruff: noqa: S608  # Why: schema is a fixed test identifier, every value is $-bound.
"""Upgrade-path attack: the mixed-fleet (rolling deploy) window.

The documented mixed-fleet window (docs/guides/upgrading.md, the
claim-epoch section; docs/guides/progress.md, the seq note): old workers
(packages built before ``01.00.18_02``) run against one database with new
workers. Three invariants must hold across the boundary:

1. A PRE-FENCE worker's terminal write still lands. The old worker's
   terminal SQL fences on ``attempt`` alone (it predates the epoch
   column and never references it); the epoch fence must not lock the
   old fleet out of finishing work.
2. A NEW consumer of the progress wire must not break on the old
   worker's repeated-head state-change events (the old publish contract
   repeated the last progress event's seq; the new one consumes a seq).
   The seq-cursor guard drops duplicates without ever dropping a
   first-seen seq, and the terminal event still arrives.
3. The NEW writer's cannot-present doctrine: a terminal write that
   cannot present a claim epoch (a ``None`` bind) no-ops through the
   attempt fence's own machinery, on PG and identically on the
   in-memory twin.

The residual window the epoch fix closes only for NEW fleets - an OLD
stale handler at the attempt ceiling shares the re-dispatched row's
``(worker, attempt)`` pair and its write is not epoch-fenced - is pinned
here as DOCUMENTED behavior (docs/guides/upgrading.md: adopt by
restarting the fleet onto the new release rather than rolling it).
"""

from __future__ import annotations

import dataclasses
import json
from collections.abc import AsyncIterator
from contextlib import AsyncExitStack
from datetime import UTC, datetime, timedelta
from typing import cast
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import asyncpg
import pytest
from pydantic import TypeAdapter

from taskq._ids import new_base62, new_job_id, new_uuid
from taskq.backend._protocol import Backend, EnqueueArgs, JobId, JobRow
from taskq.backend.clock import SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.migrate import apply_pending
from taskq.progress._events import ProgressEvent
from taskq.settings import WorkerSettings
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.jobs import make_job_row
from taskq.testing.pg import create_running_job
from taskq.testing.settings import make_integration_settings_dict
from taskq.worker.deps import open_worker_deps

pytestmark = pytest.mark.integration

# The mark_succeeded statement VERBATIM from the pre-claim-epoch tree
# (fd9ee24e, src/taskq/backend/_sql_templates.py): what an old worker
# actually sends during the rolling window. Its fence is
# ``status = 'running' AND locked_by_worker = $2 AND attempt = $8``; no
# claim_epoch conjunct exists in it.
_OLD_MARK_SUCCEEDED_SQL = """\
WITH upd AS (
    UPDATE "{s}".jobs
    SET status = 'succeeded',
        finished_at = clock_timestamp(),
        locked_by_worker = NULL,
        lock_expires_at = NULL,
        result = $3::jsonb,
        result_size_bytes = $4,
        result_expires_at = COALESCE(
            (SELECT clock_timestamp() + result_ttl * interval '1 second' FROM "{s}".actor_config WHERE actor = "{s}".jobs.actor),
            clock_timestamp() + $7::interval,
            result_expires_at
        ),
        progress_seq = $5,
        progress_state = CASE WHEN $6::jsonb IS NOT NULL THEN COALESCE(progress_state, '{{}}'::jsonb) || $6::jsonb ELSE progress_state END
    WHERE id = $1 AND status = 'running' AND locked_by_worker = $2 AND attempt = $8
    RETURNING *
), holder AS (
    SELECT id FROM "{s}".workers WHERE id = $2 FOR KEY SHARE
), att AS (
    INSERT INTO "{s}".job_attempts
    (job_id, attempt, started_at, finished_at, outcome,
     error_class, error_message, error_traceback, duration_ms, worker_id, metadata)
    SELECT upd.id, upd.attempt, upd.started_at, clock_timestamp(), 'succeeded',
           NULL, NULL, NULL,
           trunc(EXTRACT(EPOCH FROM (upd.finished_at - upd.started_at)) * 1000)::int,
           (SELECT id FROM holder), '{{}}'::jsonb
    FROM upd
    ON CONFLICT (job_id, attempt) DO NOTHING
), evt AS (
    INSERT INTO "{s}".job_events
    (job_id, occurred_at, kind, detail)
    SELECT upd.id, clock_timestamp(), 'state_change',
           jsonb_build_object('from_state', 'running', 'to_state', 'succeeded',
                              'worker_id', $2::text)
    FROM upd
)
SELECT * FROM upd"""


@pytest.fixture
async def mixed_schema(pg_dsn: str) -> AsyncIterator[str]:
    """A schema migrated by the NEW code (the rolling order: migrate
    first, then roll pods) with one seeded actor."""
    schema = f"atk_mf_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema)
        await conn.execute(
            f'INSERT INTO "{schema}".actor_config (actor, queue) '
            "VALUES ('test_actor', 'default') ON CONFLICT DO NOTHING"
        )
        yield schema
    finally:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


async def _backend(pg_dsn: str, schema: str) -> PostgresBackend:
    settings = WorkerSettings.load_from_dict(make_integration_settings_dict(pg_dsn))
    settings.schema_name = schema
    stack = AsyncExitStack()
    deps = await stack.enter_async_context(open_worker_deps(settings))
    return PostgresBackend(
        deps,
        clock=SystemClock(),
        cancellation_grace_period=timedelta(seconds=deps.settings.cancellation_grace_period),
        cleanup_grace_period=timedelta(seconds=deps.settings.cleanup_grace_period),
    )


async def test_pre_fence_terminal_write_lands_on_new_schema(pg_dsn: str, mixed_schema: str) -> None:
    """Invariant 1: the old worker's actual statement (no epoch conjunct)
    finishes a job it claimed under the new schema."""
    conn = await asyncpg.connect(pg_dsn)
    try:
        worker_id = new_uuid()
        job_id = await create_running_job(conn, mixed_schema, worker_id)
        # The old-claim shape: the old dispatch claim never touched
        # claim_epoch, so the row keeps the migration default 0. The
        # helper seeds epoch == attempt (a new-fleet history); the
        # diverged old-fleet shape seeds the two columns separately, as
        # the helper's own comment prescribes.
        await conn.execute(
            f'UPDATE "{mixed_schema}".jobs SET claim_epoch = 0 WHERE id = $1', job_id
        )
        epoch = await conn.fetchval(
            f'SELECT claim_epoch FROM "{mixed_schema}".jobs WHERE id = $1', job_id
        )
        assert epoch == 0

        landed = await conn.fetchval(
            _OLD_MARK_SUCCEEDED_SQL.format(s=mixed_schema),
            job_id,
            worker_id,
            json.dumps({"who": "old"}),
            16,
            0,
            None,
            timedelta(hours=1),
            1,  # attempt, matching the row the old worker claimed
        )
        assert landed is not None, (
            "the pre-fence worker's terminal write was locked out by the "
            "epoch fence - the rolling window breaks"
        )
        status = await conn.fetchval(
            f'SELECT status FROM "{mixed_schema}".jobs WHERE id = $1', job_id
        )
        attempts = await conn.fetchval(
            f'SELECT count(*) FROM "{mixed_schema}".job_attempts WHERE job_id = $1',
            job_id,
        )
        assert status == "succeeded"
        assert attempts == 1, "the old write must record exactly one attempt"
    finally:
        await conn.close()


async def test_ceiling_residual_old_handler_write_lands_as_documented(
    pg_dsn: str, mixed_schema: str
) -> None:
    """The DOCUMENTED residual window: an OLD stale handler at the
    attempt ceiling shares the re-dispatched row's ``(worker, attempt)``
    pair and its write is not epoch-fenced, so it lands.

    This is the defect 01.00.18_02 closes for new fleets, and the reason
    the upgrade guide's discipline is 'adopt by restarting the fleet
    onto the new release rather than rolling it'. Pinned so a future
    change that silently alters the window's shape surfaces here.
    """
    conn = await asyncpg.connect(pg_dsn)
    try:
        worker_id = new_uuid()
        # A row parked at the smallint ceiling: the claim's saturating
        # increment stops advancing the DISPLAYED attempt.
        job_id = await create_running_job(conn, mixed_schema, worker_id, attempt=32767)
        await conn.execute(
            f'UPDATE "{mixed_schema}".jobs SET claim_epoch = 5 WHERE id = $1',
            job_id,
        )
        landed = await conn.fetchval(
            _OLD_MARK_SUCCEEDED_SQL.format(s=mixed_schema),
            job_id,
            worker_id,
            json.dumps({"who": "old-stale"}),
            16,
            0,
            None,
            timedelta(hours=1),
            32767,  # the shared, saturated attempt pair
        )
        assert landed is not None, (
            "the documented residual window changed shape: an old handler "
            "at the attempt ceiling no longer lands its write"
        )
    finally:
        await conn.close()


async def test_new_writer_none_epoch_no_ops(pg_dsn: str, mixed_schema: str) -> None:
    """Invariant 3, PG side: a NEW writer that cannot present an epoch
    no-ops (the same cannot-prove-it doctrine the attempt fence uses)."""
    conn = await asyncpg.connect(pg_dsn)
    try:
        worker_id = new_uuid()
        job_id = await create_running_job(conn, mixed_schema, worker_id)
        backend = await _backend(pg_dsn, mixed_schema)
        ok = await backend.mark_succeeded(
            cast(JobId, job_id), worker_id, {"who": "new"}, claim_epoch=None
        )
        assert ok is False
        row = await conn.fetchrow(
            f'SELECT status, locked_by_worker FROM "{mixed_schema}".jobs WHERE id = $1',
            job_id,
        )
        assert row is not None
        assert row["status"] == "running", "a None-epoch write must not land"
        assert row["locked_by_worker"] == worker_id
    finally:
        await conn.close()


def _twin_args(actor: str = "test_actor") -> EnqueueArgs:
    return EnqueueArgs(
        id=new_job_id(),
        actor=actor,
        queue="default",
        payload={"v": 1},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=None,
        idempotency_key=None,
        idempotency_scope="",
        metadata={},
    )


async def test_twin_mirrors_none_epoch_and_stale_epoch() -> None:
    """Invariant 3, twin side: the in-memory backend's fence treats a
    ``None`` epoch (and a stale one) exactly as PG's equality conjuncts
    do - the two engines agree across the rolling boundary."""
    backend = InMemoryBackend(clock=FakeClock(datetime.now(UTC)))
    backend.register_actor_config(actor="test_actor")
    worker_id = new_uuid()
    row = await backend.enqueue(_twin_args())
    dispatched = await backend.dispatch_batch(
        worker_id=worker_id, queues=["default"], limit=1, lock_lease=timedelta(seconds=30)
    )
    assert len(dispatched) == 1
    claimed = dispatched[0]
    assert claimed.id == row.id
    assert claimed.claim_epoch >= 1

    # None epoch: no-op.
    ok = await backend.mark_succeeded(row.id, worker_id, {"who": "new"}, claim_epoch=None)
    assert ok is False
    # Stale epoch: fenced.
    ok = await backend.mark_succeeded(
        row.id, worker_id, {"who": "new"}, claim_epoch=claimed.claim_epoch - 1
    )
    assert ok is False
    # Presented epoch: lands.
    ok = await backend.mark_succeeded(
        row.id,
        worker_id,
        {"who": "new"},
        claim_epoch=claimed.claim_epoch,
        attempt=claimed.attempt,
    )
    assert ok is True
    stored = await backend.get(row.id)
    assert stored is not None and stored.status == "succeeded"


# ── Invariant 2: the wire ────────────────────────────────────────────────

_SCHEMA_LABEL = "atk_mf_wire"
_JOB_ID = UUID("aaaaaaaa-bbbb-cccc-dddd-000000000001")
_ACTOR = "test_actor"
_RA = TypeAdapter(type(None))


def _wire_event(
    *,
    seq: int,
    kind: str = "progress",
    status: str = "running",
    terminal: bool = False,
) -> dict[str, object]:
    event = ProgressEvent(
        kind=kind,  # type: ignore[arg-type]  # Why: test-only construction with known-valid values.
        job_id=_JOB_ID,
        actor=_ACTOR,
        ts=datetime.now(UTC),
        seq=seq,
        status=cast("object", status),
        terminal=terminal,
    )
    return {
        "type": "message",
        "data": event.model_dump_json(exclude_none=True).encode("utf-8"),
    }


async def test_new_consumer_survives_old_repeated_head_wire() -> None:
    """Invariant 2: the old worker's repeated-head state-change events on
    the wire (its seq repeats the last progress event's head; a stale one
    can even be LOWER) must not break the new consumer, must never cause
    a first-seen seq to be dropped, and the terminal event must arrive."""
    from taskq.client._handle import JobHandle

    pubsub = AsyncMock()
    messages: list[dict[str, object]] = [
        {
            "type": "subscribe",
            "data": None,
            "channel": f"taskq:{_SCHEMA_LABEL}:progress:{_JOB_ID}",
        },
    ]
    # The mixed-fleet wire, interleaved: new-worker progress at 5, 6;
    # an OLD worker's repeated-head state-change at 6 (repeats the head);
    # an OLD worker's stale state-change at 5; new progress at 7; the
    # terminal state-change consuming 8 (new contract).
    messages += [
        _wire_event(seq=5, kind="progress", status="running"),
        _wire_event(seq=6, kind="progress", status="running"),
        _wire_event(seq=6, kind="state_change", status="running"),  # old repeated-head
        _wire_event(seq=5, kind="state_change", status="running"),  # old stale
        _wire_event(seq=7, kind="progress", status="running"),
        _wire_event(seq=8, kind="state_change", status="succeeded", terminal=True),
    ]
    remaining = list(messages)

    async def _get_message(
        *,
        ignore_subscribe_messages: bool = True,
        timeout: float = 0,  # noqa: ASYNC109
    ) -> dict[str, object] | None:
        if remaining:
            return remaining.pop(0)
        return None

    pubsub.get_message = _get_message
    pubsub.listen = AsyncMock()
    pubsub.subscribe = AsyncMock()
    pubsub.unsubscribe = AsyncMock()
    pubsub.__aenter__ = AsyncMock(return_value=pubsub)
    pubsub.__aexit__ = AsyncMock(return_value=False)

    redis_client = MagicMock(spec=["pubsub"])
    redis_client.pubsub.return_value = pubsub

    row: JobRow = dataclasses.replace(
        make_job_row(status="running", progress_seq=0, actor=_ACTOR),
        id=cast(JobId, _JOB_ID),
    )
    settings = WorkerSettings.load_from_dict({"TASKQ_SCHEMA_NAME": _SCHEMA_LABEL})
    backend = AsyncMock(spec=Backend)

    async def _get(job_id: JobId) -> JobRow | None:  # pragma: no cover
        return None

    backend.get = _get

    handle = JobHandle(
        backend=backend,
        row=row,
        result_adapter=_RA,
        was_existing=False,
        _redis_client=redis_client,
        _settings=settings,
    )

    seen: list[tuple[str, int, bool]] = []
    async for event in handle.progress_stream():
        seen.append((event.kind, event.seq, event.terminal))
        if event.terminal:
            break

    kinds = [k for k, _, _ in seen]
    seqs = [s for _, s, _ in seen]
    # No first-seen seq dropped: 5, 6, 7 and both state-changes arrived.
    assert seqs, "the consumer yielded nothing on the mixed-fleet wire"
    assert 7 in seqs, "a first-seen progress seq was dropped after old repeated-head events"
    assert kinds.count("state_change") >= 3, (
        "the old repeated-head and stale state-change events must pass "
        "through without breaking the consumer"
    )
    # The stream terminates on the terminal event without error.
    assert seen[-1] == ("state_change", 8, True)


async def test_new_consumer_drops_true_progress_duplicates() -> None:
    """The guard's own contract survives the mixed fleet: duplicate
    progress seqs (the OTHER documented duplicate window, a worker dying
    mid-job and re-publishing) stay dropped; the dedupe never drops a
    seq the consumer has not seen."""
    from taskq.client._handle import JobHandle

    pubsub = AsyncMock()
    messages: list[dict[str, object]] = [
        {
            "type": "subscribe",
            "data": None,
            "channel": f"taskq:{_SCHEMA_LABEL}:progress:{_JOB_ID}",
        },
        _wire_event(seq=5, kind="progress", status="running"),
        _wire_event(seq=5, kind="progress", status="running"),  # duplicate
        _wire_event(seq=6, kind="progress", status="running"),
        _wire_event(seq=6, kind="progress", status="running"),  # duplicate
        _wire_event(seq=7, kind="state_change", status="succeeded", terminal=True),
    ]
    remaining = list(messages)

    async def _get_message(
        *,
        ignore_subscribe_messages: bool = True,
        timeout: float = 0,  # noqa: ASYNC109
    ) -> dict[str, object] | None:
        if remaining:
            return remaining.pop(0)
        return None

    pubsub.get_message = _get_message
    pubsub.listen = AsyncMock()
    pubsub.subscribe = AsyncMock()
    pubsub.unsubscribe = AsyncMock()
    pubsub.__aenter__ = AsyncMock(return_value=pubsub)
    pubsub.__aexit__ = AsyncMock(return_value=False)

    redis_client = MagicMock(spec=["pubsub"])
    redis_client.pubsub.return_value = pubsub

    row: JobRow = dataclasses.replace(
        make_job_row(status="running", progress_seq=0, actor=_ACTOR),
        id=cast(JobId, _JOB_ID),
    )
    settings = WorkerSettings.load_from_dict({"TASKQ_SCHEMA_NAME": _SCHEMA_LABEL})
    backend = AsyncMock(spec=Backend)

    async def _get(job_id: JobId) -> JobRow | None:  # pragma: no cover
        return None

    backend.get = _get

    handle = JobHandle(
        backend=backend,
        row=row,
        result_adapter=_RA,
        was_existing=False,
        _redis_client=redis_client,
        _settings=settings,
    )

    seen: list[tuple[str, int]] = []
    async for event in handle.progress_stream():
        seen.append((event.kind, event.seq))
        if event.terminal:
            break

    assert seen == [
        ("progress", 5),
        ("progress", 6),
        ("state_change", 7),
    ], f"duplicate handling drifted on the mixed-fleet wire: {seen}"

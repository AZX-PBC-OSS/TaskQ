"""Red-team attacks on the `cron_schedule_id` provenance stamp.

The round-5 fix scopes the DST twin-coverage walk to the schedule that
enqueued the jobs: every cron fire and twin carries
``metadata['cron_schedule_id']`` and
``_skip_already_delivered_overlap_twins`` counts only rows bearing the
planning schedule's own id (``cron_loop.py:719``). That closes theft
*between schedules* — but the stamp is written by a query that trusts
whatever the row carries, and the client entry point that builds those
rows lets a caller supply the key: ``build_enqueue_args`` strips a
caller-supplied ``batch_id`` as a "Security boundary" (``_args.py:225``)
and nothing else. A caller can therefore self-assert membership in a
victim schedule's delivery set with an ordinary on-demand enqueue.

The three attacks below share one hypothesis — a forged stamp counts
as coverage — pinned at each layer it crosses: the single-enqueue
entry point, the batch-enqueue entry point, and a real tick driving a
real schedule across the fold, where the forged job advances
``next_fire_at`` past an owed occurrence that then never fires.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, datetime
from uuid import UUID

import asyncpg
import pytest
from pydantic import BaseModel

from taskq import actor
from taskq._ids import new_uuid
from taskq.batch import EnqueueItem
from taskq.client._args import build_batch_args, build_enqueue_args
from taskq.settings import WorkerSettings
from taskq.testing.fixtures import ModulePgSchema
from taskq.worker.cron_loop import ActorFirePolicy, tick_cron

from .test_rt_cron_harness import (
    count_jobs,
    cron_settings,
    make_backend,
    schedule_row,
    seed_actor_config,
    seed_schedule,
)

_ACTOR = "rt_stamp_forgery_open"

# 2026-11-01 fall-back (02:00 EDT -> 01:00 EST): fold-0 runs
# 05:00-05:59 UTC, fold-1 06:00-06:59 UTC, past-the-range 07:00 UTC.
_LAST_FOLD0_TICK_UTC = datetime(2026, 11, 1, 5, 59, tzinfo=UTC)
_FIRST_FOLD1_SLOT_UTC = datetime(2026, 11, 1, 6, 0, tzinfo=UTC)


class _DecoyPayload(BaseModel):
    value: int = 1


@actor(name=_ACTOR)
async def _forgery_actor(_payload: _DecoyPayload) -> None:
    pass


def test_caller_supplied_cron_schedule_id_is_stripped_from_single_enqueue() -> None:
    """A single enqueue must not self-assert a cron provenance stamp.

    ``batch_id`` is stripped at this exact boundary as a security
    measure against self-asserted batch membership; ``cron_schedule_id``
    buys the same power over a schedule's delivery set (the coverage
    walk takes it as proof an occurrence was delivered) and is stripped
    nowhere. The test pins the stamp absent while ordinary user
    metadata survives.
    """
    victim = str(new_uuid())
    args = build_enqueue_args(
        _forgery_actor,
        _DecoyPayload(),
        metadata={"cron_schedule_id": victim, "note": "user-data"},
    )
    assert "cron_schedule_id" not in args.metadata, (
        "a caller-supplied cron_schedule_id must not reach the job row — "
        f"it lets any enqueue pose as schedule {victim}'s delivery"
    )
    assert args.metadata.get("note") == "user-data"


def test_caller_supplied_cron_schedule_id_is_stripped_from_batch_items() -> None:
    """The batch entry point must strip the stamp before stamping the batch.

    ``build_batch_args`` delegates to ``build_enqueue_args`` (which
    strips ``batch_id``) and then re-stamps the library's own
    ``batch_id`` afterwards. A forged ``cron_schedule_id`` on an item
    survives both steps today: the strip does not know the key and the
    re-stamp only adds ``batch_id``. Both items below must lose the
    forgery while keeping the genuine batch stamp.
    """
    victim = str(new_uuid())
    items = [
        EnqueueItem(
            actor_ref=_forgery_actor,
            payload=_DecoyPayload(value=i),
            metadata={"cron_schedule_id": victim},
        )
        for i in range(2)
    ]
    built = build_batch_args(items, new_uuid())
    assert len(built) == 2
    for args in built:
        assert "cron_schedule_id" not in args.metadata, (
            "a caller-supplied cron_schedule_id must not reach the job row — "
            f"it lets any batch item pose as schedule {victim}'s delivery"
        )
        assert "batch_id" in args.metadata


class _PinnedDueConn:
    """Due-bound-pinning wrapper (same shape as the parity-DST drive's).

    Only the tick's driving SELECT is rewritten to the pinned instant;
    every other statement — planning clock, preflights, batched enqueue,
    schedule UPDATEs — runs against real PG on the same connection.
    """

    def __init__(self, conn: asyncpg.Connection, due_as_of: datetime) -> None:
        self._conn = conn
        self._due_as_of = due_as_of

    async def fetch(self, sql: str, *args: object) -> list[asyncpg.Record]:
        if "next_fire_at <= statement_timestamp()" in sql:
            sql = sql.replace(
                "statement_timestamp()",
                f"'{self._due_as_of.isoformat()}'::timestamptz",
            )
        return await self._conn.fetch(sql, *args)

    async def fetchrow(self, sql: str, *args: object) -> asyncpg.Record | None:
        return await self._conn.fetchrow(sql, *args)

    async def fetchval(self, sql: str, *args: object) -> object | None:
        return await self._conn.fetchval(sql, *args)

    async def execute(self, sql: str, *args: object) -> object:
        return await self._conn.execute(sql, *args)

    def __getattr__(self, name: str) -> object:
        return getattr(self._conn, name)


async def _tick(
    conn: asyncpg.Connection,
    settings: WorkerSettings,
    schema: str,
    policies: Mapping[str, ActorFirePolicy],
    *,
    due_as_of: datetime,
) -> int:
    async with conn.transaction():
        return await tick_cron(
            _PinnedDueConn(conn, due_as_of),  # type: ignore[arg-type]  # Why: the wrapper implements the connection's awaited protocol surface used by the tick; pyright cannot see through __getattr__ delegation.
            settings,
            make_backend(settings),
            schema,
            new_uuid(),
            actor_policies=policies,
        )


@pytest.mark.integration
async def test_forged_provenance_stamp_steals_an_owed_fold_occurrence(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """A forged stamp on an on-demand job skips a schedule's owed fire.

    The victim is a minutely ``allof`` schedule seeded on the last
    fold-0 slot: it holds no twins and owes the whole fold-1 pass, so
    after firing 01:59 fold-0 its next fire must be the pass's first
    match (06:00 UTC). The decoy is an ordinary on-demand job for the
    same actor at exactly that instant, requesting the victim's
    schedule id in its metadata through ``build_enqueue_args`` — the
    row carries whatever the public entry point preserves, so this
    test drives the exact public path rather than hand-stamping the
    row. While the boundary lets the stamp through, the tick still
    fires honestly (one plan) but the coverage walk takes the decoy for
    the schedule's own delivery and advances ``next_fire_at`` to 06:01:
    the 06:00 occurrence is never enqueued and never fires — one
    occurrence silently lost per forged instant, with no error anywhere.
    Once the boundary strips the stamp, the decoy is inert and the
    schedule fires 06:00 itself.
    """
    schema = module_pg_schema.schema_name
    settings = cron_settings(schema)
    await seed_actor_config(clean_pg_conn, schema, _ACTOR)
    schedule_id: UUID = await seed_schedule(
        clean_pg_conn,
        schema,
        actor=_ACTOR,
        name="stamp-forgery-victim",
        cron_expr="* * * * *",
        timezone="America/New_York",
        dst_strategy="allof",
        next_fire_at=_LAST_FOLD0_TICK_UTC,
        identity_key="stamp-forgery-victim",
    )

    forged = build_enqueue_args(
        _forgery_actor,
        _DecoyPayload(),
        scheduled_at=_FIRST_FOLD1_SLOT_UTC,
        metadata={"cron_schedule_id": str(schedule_id)},
    )
    await clean_pg_conn.execute(
        f'INSERT INTO "{schema}".jobs '  # noqa: S608  # Why: schema is a test-fixture identifier; every value is $-bound.
        "(id, actor, queue, payload, max_attempts, retry_kind, status, scheduled_at, metadata) "
        f"VALUES ($1, $2, 'rt_queue', $3::jsonb, 5, 'transient', "
        f"'scheduled'::\"{schema}\".job_status, $4, $5::jsonb)",
        forged.id,
        forged.actor,
        json.dumps(forged.payload),
        forged.scheduled_at,
        json.dumps(dict(forged.metadata)),
    )

    fired = await _tick(clean_pg_conn, settings, schema, {}, due_as_of=_LAST_FOLD0_TICK_UTC)
    assert fired == 1, (
        "the last fold-0 tick owes its own fire — a failure here is setup, not the finding"
    )
    row = await schedule_row(clean_pg_conn, schema, schedule_id)
    assert row["next_fire_at"] == _FIRST_FOLD1_SLOT_UTC, (
        "the schedule holds no twins and owes the whole fold-1 pass; its "
        "next owed occurrence is the pass's first match "
        f"({_FIRST_FOLD1_SLOT_UTC.isoformat()}) — an on-demand job's forged "
        f"stamp is not its delivery — got {row['next_fire_at'].isoformat()}"
    )
    assert await count_jobs(clean_pg_conn, schema, _ACTOR) == 2, (
        "exactly the honest fire plus the decoy: no owed occurrence may be "
        "conjured away by forged metadata"
    )

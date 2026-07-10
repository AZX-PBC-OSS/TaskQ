"""Cron schedule CRUD for PostgresBackend.

The schedule create/list/update/delete operations are thin asyncpg
wrappers.  They live here as module-level functions taking the worker
pool, a pre-rendered :class:`ScheduleSql` bundle, and the schedule id,
so :class:`~taskq.backend.postgres.PostgresBackend` can delegate without
exposing its private SQL templates across module boundaries.
"""

from dataclasses import dataclass
from typing import TYPE_CHECKING
from uuid import UUID

from taskq._json import loads
from taskq.backend._protocol import (
    ScheduleCreateArgs,
    ScheduleRecord,
    ScheduleUpdateArgs,
)
from taskq.backend._records import jsonb_param

if TYPE_CHECKING:
    import asyncpg

__all__ = [
    "ScheduleSql",
    "create_schedule",
    "delete_schedule",
    "list_schedules",
    "schedule_record_from_record",
    "update_schedule",
]

# Schedule SQL templates.  ``{schema}`` is interpolated via ``.format`` at
# build time (schema is validated against _IDENT_RE by the caller), which
# keeps the surface free of f-string S608 noise.
_SCHEDULE_CREATE_SQL = """\
INSERT INTO "{schema}".cron_schedules
(id, actor, name, cron_expr, timezone, dst_strategy, payload_factory, enabled,
 next_fire_at, identity_key, metadata)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb)
RETURNING *"""

_SCHEDULE_LIST_SQL = """\
SELECT * FROM "{schema}".cron_schedules"""

_SCHEDULE_LIST_WHERE_ACTOR_SQL = """\
SELECT * FROM "{schema}".cron_schedules WHERE actor = $1"""

_SCHEDULE_LIST_WHERE_ENABLED_SQL = """\
SELECT * FROM "{schema}".cron_schedules WHERE enabled = $1"""

_SCHEDULE_LIST_WHERE_ACTOR_ENABLED_SQL = """\
SELECT * FROM "{schema}".cron_schedules WHERE actor = $1 AND enabled = $2"""

_SCHEDULE_UPDATE_SQL = """\
UPDATE "{schema}".cron_schedules SET"""

_SCHEDULE_DELETE_SQL = """\
DELETE FROM "{schema}".cron_schedules WHERE id = $1"""

_SCHEDULE_SELECT_BY_ID_SQL = """\
SELECT * FROM "{schema}".cron_schedules WHERE id = $1"""


@dataclass(frozen=True, slots=True)
class ScheduleSql:
    """Pre-rendered SQL strings for the cron_schedules table."""

    create: str
    list_all: str
    list_where_actor: str
    list_where_enabled: str
    list_where_actor_enabled: str
    update: str
    delete: str
    select_by_id: str

    @staticmethod
    def build(schema: str) -> "ScheduleSql":
        return ScheduleSql(
            create=_SCHEDULE_CREATE_SQL.format(schema=schema),
            list_all=_SCHEDULE_LIST_SQL.format(schema=schema),
            list_where_actor=_SCHEDULE_LIST_WHERE_ACTOR_SQL.format(schema=schema),
            list_where_enabled=_SCHEDULE_LIST_WHERE_ENABLED_SQL.format(schema=schema),
            list_where_actor_enabled=_SCHEDULE_LIST_WHERE_ACTOR_ENABLED_SQL.format(schema=schema),
            update=_SCHEDULE_UPDATE_SQL.format(schema=schema),
            delete=_SCHEDULE_DELETE_SQL.format(schema=schema),
            select_by_id=_SCHEDULE_SELECT_BY_ID_SQL.format(schema=schema),
        )


def schedule_record_from_record(rec: "asyncpg.Record") -> ScheduleRecord:
    """Convert an asyncpg Record from ``cron_schedules`` into ScheduleRecord."""
    d: dict[str, object] = dict(rec)
    raw_meta: object = d.get("metadata")
    if raw_meta is not None and not isinstance(raw_meta, dict):
        d["metadata"] = loads(str(raw_meta))
    return ScheduleRecord.model_validate(d)


async def create_schedule(
    pool: "asyncpg.Pool",
    sql: ScheduleSql,
    args: ScheduleCreateArgs,
) -> ScheduleRecord:
    """Insert a row into ``cron_schedules`` and return it.

    Does NOT suppress ``UniqueViolationError`` — callers handle it
    (the ``(actor, name)`` UNIQUE constraint).  ``next_fire_at`` is provided by the
    caller (computed client-side via ``compute_next_fire_after``).
    """
    from taskq._ids import new_uuid

    sid = new_uuid()
    metadata_json = jsonb_param(args.metadata) or "{}"
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            sql.create,
            sid,
            args.actor,
            args.name,
            args.cron_expr,
            args.timezone,
            args.dst_strategy,
            args.payload_factory,
            args.enabled,
            args.next_fire_at,
            args.identity_key,
            metadata_json,
        )
    assert row is not None
    return schedule_record_from_record(row)


async def list_schedules(
    pool: "asyncpg.Pool",
    sql: ScheduleSql,
    *,
    actor: str | None = None,
    enabled: bool | None = None,
) -> list[ScheduleRecord]:
    """Query ``cron_schedules`` with optional WHERE predicates."""
    async with pool.acquire() as conn:
        if actor is not None and enabled is not None:
            rows = await conn.fetch(sql.list_where_actor_enabled, actor, enabled)
        elif actor is not None:
            rows = await conn.fetch(sql.list_where_actor, actor)
        elif enabled is not None:
            rows = await conn.fetch(sql.list_where_enabled, enabled)
        else:
            rows = await conn.fetch(sql.list_all)
    return [schedule_record_from_record(r) for r in rows]


async def update_schedule(
    pool: "asyncpg.Pool",
    sql: ScheduleSql,
    schedule_id: UUID,
    args: ScheduleUpdateArgs,
) -> ScheduleRecord:
    """Update a cron schedule row.  Returns the updated row.

    When ``enabled=True``: the UPDATE also sets
    ``consecutive_failures = 0`` and ``last_fire_error = NULL``.
    When ``cron_expr`` is provided: caller must also provide
    ``next_fire_at`` (recomputed via ``compute_next_fire_after``).
    """
    sets: list[str] = []
    params: list[object] = []
    idx = 1

    if args.cron_expr is not None:
        idx += 1
        sets.append(f"cron_expr = ${idx}")
        params.append(args.cron_expr)
    if args.next_fire_at is not None:
        idx += 1
        sets.append(f"next_fire_at = ${idx}")
        params.append(args.next_fire_at)
    if args.enabled is not None:
        idx += 1
        sets.append(f"enabled = ${idx}")
        params.append(args.enabled)
        if args.enabled:
            sets.append("consecutive_failures = 0")
            sets.append("last_fire_error = NULL")
    if args.payload_factory is not None:
        idx += 1
        sets.append(f"payload_factory = ${idx}")
        params.append(args.payload_factory)
    elif args.clear_payload_factory:
        sets.append("payload_factory = NULL")
    if args.metadata is not None:
        idx += 1
        sets.append(f"metadata = ${idx}::jsonb")
        params.append(jsonb_param(args.metadata) or "{}")
    if args.consecutive_failures is not None:
        idx += 1
        sets.append(f"consecutive_failures = ${idx}")
        params.append(args.consecutive_failures)
    if args.last_fire_error is not None:
        idx += 1
        sets.append(f"last_fire_error = ${idx}")
        params.append(args.last_fire_error)

    if not sets:
        # No fields to update — return current row.
        async with pool.acquire() as conn:
            row = await conn.fetchrow(sql.select_by_id, schedule_id)
        if row is None:
            raise KeyError(f"schedule {schedule_id} not found")
        return schedule_record_from_record(row)

    final_sql = f"{sql.update} {', '.join(sets)} WHERE id = $1 RETURNING *"
    async with pool.acquire() as conn:
        row = await conn.fetchrow(final_sql, schedule_id, *params)
    if row is None:
        raise KeyError(f"schedule {schedule_id} not found")
    return schedule_record_from_record(row)


async def delete_schedule(pool: "asyncpg.Pool", sql: ScheduleSql, schedule_id: UUID) -> None:
    """Delete a cron schedule.  Idempotent — no error if row missing."""
    async with pool.acquire() as conn:
        await conn.execute(sql.delete, schedule_id)

"""Enqueue operations for PostgresBackend.

``enqueue``, ``enqueue_with_conn``, ``enqueue_batch``, and
``enqueue_batch_fast`` live here as module-level functions taking
``(pool, sql: SqlTemplates, schema, clock, ...)`` parameters.
:class:`~taskq.backend.postgres.PostgresBackend` methods are thin
wrappers that delegate.
"""

from datetime import datetime
from typing import TYPE_CHECKING
from uuid import UUID

import structlog
from asyncpg.exceptions import UniqueViolationError

from taskq._json import dumps_str
from taskq.backend._protocol import (
    ConnLike,
    EnqueueArgs,
    JobRow,
)
from taskq.backend._records import (
    _job_row_from_record,
    jsonb_param,
)
from taskq.backend._sql_templates import SqlTemplates
from taskq.backend.clock import Clock
from taskq.constants import wake_channel
from taskq.exceptions import (
    MaxPendingExceededError,
    SingletonCollisionError,
)
from taskq.obs import (
    get_logger,
    record_backpressure_error,
)

if TYPE_CHECKING:
    import asyncpg

__all__ = [
    "_enqueue",
    "_enqueue_batch",
    "_enqueue_batch_fast",
    "_enqueue_on_conn",
    "_enqueue_with_conn",
]

logger: structlog.stdlib.BoundLogger = get_logger(__name__)

_SINGLETON_CONSTRAINT_NAME = "jobs_singleton_uniq"


async def _enqueue_on_conn(
    conn: ConnLike,
    sql: SqlTemplates,
    schema: str,
    clock: Clock,
    args: EnqueueArgs,
) -> JobRow:
    """Core enqueue logic running on *conn*.

    Includes unique_for preflight, singleton preflight, max_pending
    count, INSERT, idempotency-key SELECT on conflict, and pg_notify.
    Does NOT acquire from ``worker_pool`` and does NOT open a
    transaction — the caller is responsible for both.
    """
    if args.unique_for is not None and args.identity_key is not None:
        existing_rec = await conn.fetchrow(
            sql.enqueue_unique_for_preflight,
            args.actor,
            args.identity_key,
            list(args.unique_states),
            args.unique_for,
        )
        if existing_rec is not None:
            row = _job_row_from_record(existing_rec)
            logger.info(
                "enqueue_deduplicated",
                kind="enqueue_deduplicated",
                job_id=str(row.id),
                actor=row.actor,
                queue=row.queue,
                identity_key=row.identity_key,
                idempotency_key=None,
                existing_job_id=str(row.id),
                dedup_reason="unique_for",
            )
            return row

    if args.metadata.get("singleton") is True:
        preflight_rec = await conn.fetchrow(sql.singleton_preflight, args.actor)
        if preflight_rec is not None:
            blocking_id: UUID = preflight_rec["id"]
            schedule_to_close: datetime | None = preflight_rec["schedule_to_close"]
            retry_after = None
            if schedule_to_close is not None:
                now_utc = clock.now()
                remaining = schedule_to_close - now_utc
                if remaining.total_seconds() > 0:
                    retry_after = remaining
            logger.info(
                "singleton-collision",
                actor=args.actor,
                blocking_job_id=str(blocking_id),
                detection_path="preflight_check",
            )
            raise SingletonCollisionError(
                actor=args.actor,
                blocking_job_id=blocking_id,
                retry_after=retry_after,
            )

    if args.max_pending is not None:
        count_rec = await conn.fetchval(
            sql.enqueue_max_pending_count,
            args.actor,
        )
        current_count: int = int(count_rec)
        if current_count >= args.max_pending:
            logger.warning(
                "max-pending-exceeded",
                actor=args.actor,
                current_count=current_count,
                max_pending=args.max_pending,
            )
            record_backpressure_error(args.actor, kind="max_pending")
            raise MaxPendingExceededError(
                actor=args.actor,
                current_count=current_count,
                max_pending=args.max_pending,
            )

    is_new = False
    use_interval = args.schedule_to_close_interval is not None
    sql_stmt = sql.enqueue_with_interval if use_interval else sql.enqueue
    param_12: object = args.schedule_to_close_interval if use_interval else args.schedule_to_close
    enqueue_now = clock.now()
    scheduled_at_param: datetime | None = (
        args.scheduled_at if args.scheduled_at > enqueue_now else None
    )
    result_expires_at: datetime | None = None
    if args.result_ttl is not None:
        result_expires_at = enqueue_now + args.result_ttl

    try:
        rec = await conn.fetchrow(
            sql_stmt,
            args.id,
            args.actor,
            args.queue,
            args.identity_key,
            args.fairness_key,
            jsonb_param(args.payload),
            args.payload_schema_ver,
            args.priority,
            args.max_attempts,
            args.retry_kind,
            param_12,
            args.start_to_close,
            args.heartbeat_timeout,
            scheduled_at_param,
            args.idempotency_key,
            args.trace_id,
            args.span_id,
            jsonb_param(args.metadata),
            result_expires_at,
            list(args.tags),
        )
    except UniqueViolationError as exc:
        if exc.constraint_name == _SINGLETON_CONSTRAINT_NAME:
            logger.info(
                "singleton-collision",
                actor=args.actor,
                blocking_job_id=None,
                detection_path="unique_violation_catch",
            )
            raise SingletonCollisionError(
                actor=args.actor,
                blocking_job_id=None,
                retry_after=None,
            ) from exc
        raise
    if rec is not None:
        is_new = True
    else:
        rec = await conn.fetchrow(
            sql.enqueue_select_by_key,
            args.idempotency_key,
        )
        if rec is None:
            raise RuntimeError(
                "enqueue ON CONFLICT fired but follow-up SELECT "
                f"found no row for idempotency_key={args.idempotency_key!r}"
            )

    row = _job_row_from_record(rec)

    if is_new:
        await conn.execute(
            sql.enqueue_notify,
            wake_channel(schema),
        )
        logger.info(
            "enqueue",
            kind="enqueue",
            job_id=str(row.id),
            actor=row.actor,
            queue=row.queue,
            idempotency_key=row.idempotency_key,
        )
    else:
        logger.info(
            "enqueue_deduplicated",
            kind="enqueue_deduplicated",
            job_id=str(row.id),
            actor=row.actor,
            queue=row.queue,
            identity_key=row.identity_key,
            idempotency_key=row.idempotency_key,
            existing_job_id=str(row.id),
            dedup_reason="idempotency_key",
        )

    return row


async def _enqueue_with_conn(
    conn: ConnLike,
    sql: SqlTemplates,
    schema: str,
    clock: Clock,
    args: EnqueueArgs,
) -> JobRow:
    return await _enqueue_on_conn(conn, sql, schema, clock, args)


async def _enqueue(
    pool: "asyncpg.Pool",
    sql: SqlTemplates,
    schema: str,
    clock: Clock,
    args: EnqueueArgs,
) -> JobRow:
    async with pool.acquire() as conn:
        async with conn.transaction():
            return await _enqueue_on_conn(conn, sql, schema, clock, args)


async def _enqueue_batch(
    pool: "asyncpg.Pool",
    sql: SqlTemplates,
    schema: str,
    clock: Clock,
    args_list: list[EnqueueArgs],
    *,
    connection: "asyncpg.Connection | None" = None,
) -> list[JobRow]:
    if not args_list:
        raise ValueError("args_list must not be empty")

    ids: list[UUID] = []
    actors: list[str] = []
    queues: list[str] = []
    identity_keys: list[str | None] = []
    fairness_keys: list[str | None] = []
    payloads: list[str] = []
    payload_schema_vers: list[int] = []
    priorities: list[int] = []
    max_attempts_list: list[int] = []
    retry_kinds: list[str] = []
    schedule_to_closes: list[object] = []
    start_to_closes: list[object] = []
    heartbeat_timeouts: list[object] = []
    scheduled_ats: list[datetime | None] = []
    metadatas: list[str] = []
    idempotency_keys: list[str | None] = []
    trace_ids: list[str | None] = []
    span_ids: list[str | None] = []
    result_expires_ats: list[datetime | None] = []
    tag_jsons: list[str] = []

    batch_now = clock.now()

    for args in args_list:
        ids.append(UUID(bytes=args.id.bytes))
        actors.append(args.actor)
        queues.append(args.queue)
        identity_keys.append(str(args.identity_key) if args.identity_key is not None else None)
        fairness_keys.append(args.fairness_key)
        payloads.append(jsonb_param(args.payload) or "{}")
        payload_schema_vers.append(args.payload_schema_ver)
        priorities.append(args.priority)
        max_attempts_list.append(args.max_attempts)
        retry_kinds.append(args.retry_kind)
        if args.schedule_to_close_interval is not None:
            schedule_to_closes.append(args.scheduled_at + args.schedule_to_close_interval)
        else:
            schedule_to_closes.append(args.schedule_to_close)
        start_to_closes.append(args.start_to_close)
        heartbeat_timeouts.append(args.heartbeat_timeout)
        scheduled_ats.append(args.scheduled_at if args.scheduled_at > batch_now else None)
        metadatas.append(jsonb_param(args.metadata) or "{}")
        idempotency_keys.append(
            str(args.idempotency_key) if args.idempotency_key is not None else None
        )
        trace_ids.append(args.trace_id)
        span_ids.append(args.span_id)
        result_expires_at: datetime | None = None
        if args.result_ttl is not None:
            result_expires_at = batch_now + args.result_ttl
        result_expires_ats.append(result_expires_at)
        tag_jsons.append(dumps_str(list(args.tags)))

    async def _enqueue_batch_on_conn(conn: ConnLike) -> list[JobRow]:
        returning_recs = await conn.fetch(
            sql.enqueue_batch,
            ids,
            actors,
            queues,
            identity_keys,
            fairness_keys,
            payloads,
            payload_schema_vers,
            priorities,
            max_attempts_list,
            retry_kinds,
            schedule_to_closes,
            start_to_closes,
            heartbeat_timeouts,
            scheduled_ats,
            metadatas,
            idempotency_keys,
            trace_ids,
            span_ids,
            result_expires_ats,
            tag_jsons,
        )

        inserted_ids: set[UUID] = {rec["id"] for rec in returning_recs}
        if inserted_ids:
            await conn.execute(
                sql.enqueue_notify,
                wake_channel(schema),
            )

        new_rows_by_id: dict[UUID, object] = {rec["id"]: rec for rec in returning_recs}

        collision_keys: list[str] = []
        for args in args_list:
            if args.idempotency_key is not None and UUID(bytes=args.id.bytes) not in inserted_ids:
                collision_keys.append(str(args.idempotency_key))

        new_item_ids = list(inserted_ids)
        full_new_recs: dict[UUID, object] = {}
        if new_item_ids:
            recs = await conn.fetch(
                sql.enqueue_batch_fetch_by_ids,
                new_item_ids,
            )
            for rec in recs:
                full_new_recs[UUID(bytes=rec["id"].bytes)] = rec

        existing_by_idem: dict[str, object] = {}
        if collision_keys:
            recs = await conn.fetch(
                sql.enqueue_batch_fetch_existing,
                collision_keys,
            )
            for rec in recs:
                idem_key = rec["idempotency_key"]
                existing_by_idem[idem_key] = rec

        result: list[JobRow] = []
        for args in args_list:
            arg_uuid = UUID(bytes=args.id.bytes)
            if arg_uuid in full_new_recs:
                result.append(_job_row_from_record(full_new_recs[arg_uuid]))  # type: ignore[arg-type]  # Why: asyncpg Record is duck-typed; _job_row_from_record accepts asyncpg.Record at runtime
            elif args.idempotency_key is not None and str(args.idempotency_key) in existing_by_idem:
                result.append(_job_row_from_record(existing_by_idem[str(args.idempotency_key)]))  # type: ignore[arg-type]  # Why: asyncpg Record is duck-typed; _job_row_from_record accepts asyncpg.Record at runtime
            else:
                partial = new_rows_by_id.get(arg_uuid)
                if partial is not None:
                    result.append(_job_row_from_record(partial))  # type: ignore[arg-type]  # Why: asyncpg Record is duck-typed; _job_row_from_record accepts asyncpg.Record at runtime
                else:
                    raise RuntimeError(
                        f"enqueue_batch: no row found for args.id={args.id!r} "
                        f"after INSERT; this is a bug"
                    )
        return result

    if connection is not None:
        return await _enqueue_batch_on_conn(connection)
    async with pool.acquire() as conn:
        async with conn.transaction():
            return await _enqueue_batch_on_conn(conn)


async def _enqueue_batch_fast(
    pool: "asyncpg.Pool",
    sql: SqlTemplates,
    schema: str,
    clock: Clock,
    args_list: list[EnqueueArgs],
    *,
    connection: "asyncpg.Connection | None" = None,
) -> int:
    if not args_list:
        raise ValueError("args_list must not be empty")

    batch_now = clock.now()

    records: list[tuple[object, ...]] = []
    for args in args_list:
        is_scheduled = args.scheduled_at > batch_now
        status = "scheduled" if is_scheduled else "pending"
        scheduled_at = args.scheduled_at if args.scheduled_at > batch_now else batch_now

        resolved_stc: datetime | None
        if args.schedule_to_close_interval is not None:
            resolved_stc = args.scheduled_at + args.schedule_to_close_interval
        else:
            resolved_stc = args.schedule_to_close

        result_expires_at: datetime | None = None
        if args.result_ttl is not None:
            result_expires_at = batch_now + args.result_ttl

        records.append(
            (
                UUID(bytes=args.id.bytes),
                args.actor,
                args.queue,
                str(args.identity_key) if args.identity_key is not None else None,
                args.fairness_key,
                jsonb_param(args.payload) or "{}",
                args.payload_schema_ver,
                status,
                args.priority,
                0,
                args.max_attempts,
                args.retry_kind,
                resolved_stc,
                args.start_to_close,
                args.heartbeat_timeout,
                batch_now,
                scheduled_at,
                None,
                None,
                None,
                None,
                None,
                None,
                0,
                None,
                None,
                None,
                "{}",
                0,
                None,
                None,
                result_expires_at,
                str(args.idempotency_key) if args.idempotency_key is not None else None,
                args.trace_id,
                args.span_id,
                jsonb_param(args.metadata) or "{}",
                list(args.tags),
            )
        )

    async def _copy_on_conn(conn: ConnLike) -> int:
        result = await conn.copy_records_to_table(
            "jobs",
            records=records,
            columns=sql.copy_from_columns,
            schema_name=schema,
        )
        count = int(result.split()[-1])
        await conn.execute(
            sql.enqueue_notify,
            wake_channel(schema),
        )
        return count

    if connection is not None:
        return await _copy_on_conn(connection)
    async with pool.acquire() as conn:
        async with conn.transaction():
            return await _copy_on_conn(conn)

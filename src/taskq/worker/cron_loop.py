"""Cron tick loop: firing due schedules with advisory lock and miss-handling.

Extracted from :mod:`taskq.worker.leader` per file-size
ceiling.  The leader's ``_cron_loop`` method delegates to :func:`tick_cron`
each second; :func:`fire_schedule` and :func:`resolve_payload` contain the
per-schedule fire logic.
"""

from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID

import asyncpg
import structlog
from opentelemetry import trace
from opentelemetry.trace import SpanKind, StatusCode

from taskq._ids import new_job_id
from taskq.backend._protocol import Backend, DstStrategy, EnqueueArgs, IdentityKey, parse_retry_kind
from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: reusing the canonical identifier regex rather than redefining
    CRON_LOCK_NAME,
)
from taskq.cron import (
    compute_next_fire_after,
)
from taskq.cron import (
    resolve_payload as resolve_cron_payload,
)
from taskq.obs import (
    get_logger,
    record_cron_failure,
    record_published_message,
    safe_start_span,
    update_disabled_schedules_count,
)
from taskq.settings import WorkerSettings

log: structlog.stdlib.BoundLogger = get_logger(__name__)


@dataclass(frozen=True, slots=True)
class _ActorConfig:
    queue: str
    max_attempts: int
    retry_kind: str


async def resolve_payload(row: asyncpg.Record) -> dict[str, object]:
    """Resolve payload from ``payload_factory`` or ``static_payload``.

    Delegates to :func:`~taskq.cron.resolve_payload`.  All exceptions
    (including ``TypeError`` from a factory returning an unexpected type)
    propagate to the caller so that ``fire_schedule``'s error handler
    increments ``consecutive_failures`` and triggers auto-disable.
    """
    pf: str | None = row["payload_factory"]
    return await resolve_cron_payload(pf, row["metadata"])


async def tick_cron(
    conn: asyncpg.Connection,
    settings: WorkerSettings,
    backend: Backend,
    schema: str,
    worker_id: UUID,
) -> None:
    """Fire due cron schedules.  Holds ``pg_try_advisory_xact_lock`` to prevent
    double-fire during leader handover.

    *conn* MUST already be in an open transaction — the advisory lock is
    transaction-scoped and releases on COMMIT/ROLLBACK.
    """
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")

    lock_acquired: bool = await conn.fetchval(
        "SELECT pg_try_advisory_xact_lock(hashtextextended($1, 0))",
        CRON_LOCK_NAME,
    )
    if not lock_acquired:
        return

    _cron_tick_sql = (
        f"SELECT id, actor, cron_expr, timezone, payload_factory, "
        f"metadata, last_fired_at, consecutive_failures, next_fire_at, identity_key "
        f'FROM "{schema}".cron_schedules '
        f"WHERE enabled = true AND next_fire_at <= now() "
        f"ORDER BY next_fire_at"
    )
    rows = await conn.fetch(_cron_tick_sql)
    now = datetime.now(UTC)

    actor_config_cache: dict[str, _ActorConfig] = {}
    for row in rows:
        await fire_schedule(
            conn, row, now, settings, backend, schema, worker_id, actor_config_cache
        )


async def fire_schedule(
    conn: asyncpg.Connection,
    row: asyncpg.Record,
    now: datetime,
    settings: WorkerSettings,
    backend: Backend,
    schema: str,
    worker_id: UUID,
    actor_config_cache: dict[str, _ActorConfig],
) -> None:
    """Fire a single due schedule, handling miss, payload resolution, enqueue,
    success/error UPDATE branches, OTel spans, and metrics.

    Two separate UPDATE branches for auto-disable vs non-disable (anti-pattern #3:
    ``enabled = NOT $4`` would incorrectly re-enable an already-enabled schedule).
    """
    catch_up_cutoff = now - settings.cron_catch_up_window
    fire_at: datetime = row["next_fire_at"]
    dst_strategy_raw: str = row.get("dst_strategy", "skip") or "skip"
    dst_strategy: DstStrategy = (
        dst_strategy_raw if dst_strategy_raw in ("skip", "firstof", "allof") else "skip"
    )
    if fire_at < catch_up_cutoff:
        fire_at = compute_next_fire_after(
            row["cron_expr"], row["timezone"], now, dst_strategy=dst_strategy
        )[0]
        log.warning(
            "cron missed slots skipped",
            kind="cron_fire",
            actor=row["actor"],
            schedule_id=str(row["id"]),
        )

    current_span = trace.get_current_span()
    current_ctx = current_span.get_span_context()
    links = [trace.Link(current_ctx)] if current_ctx.is_valid else None

    published_queue: str | None = None

    with safe_start_span(
        "cron fire",
        kind=SpanKind.PRODUCER,
        attributes={"cron_schedule_name": row["actor"]},
        links=links,
        new_root=True,
    ) as span:
        try:
            if row["actor"] not in actor_config_cache:
                ac_row = await conn.fetchrow(
                    f'SELECT queue, max_attempts, retry_kind FROM "{schema}".actor_config WHERE actor = $1',
                    row["actor"],
                )
                if ac_row is None:
                    raise LookupError(f"Actor '{row['actor']}' not found in actor_config")
                actor_config_cache[row["actor"]] = _ActorConfig(
                    queue=ac_row["queue"],
                    max_attempts=ac_row["max_attempts"],
                    retry_kind=ac_row["retry_kind"],
                )
            ac = actor_config_cache[row["actor"]]
            actor: str = row["actor"]

            identity_key_raw: object = row.get("identity_key")
            schedule_identity_key: IdentityKey | None = (
                IdentityKey(str(identity_key_raw)) if identity_key_raw is not None else None
            )

            payload = await resolve_payload(row)

            enqueue_args = EnqueueArgs(
                id=new_job_id(),
                actor=actor,
                queue=ac.queue,
                payload=payload,
                max_attempts=ac.max_attempts,
                retry_kind=parse_retry_kind(ac.retry_kind),
                scheduled_at=datetime.now(UTC),
                payload_schema_ver=1,
                identity_key=schedule_identity_key,
            )
            await backend.enqueue_with_conn(conn, enqueue_args)

            next_fires = compute_next_fire_after(
                row["cron_expr"], row["timezone"], fire_at, dst_strategy=dst_strategy
            )
            next_fire = next_fires[0]

            if len(next_fires) > 1 and dst_strategy == "allof":
                overlap_enqueue = EnqueueArgs(
                    id=new_job_id(),
                    actor=actor,
                    queue=ac.queue,
                    payload=payload,
                    max_attempts=ac.max_attempts,
                    retry_kind=parse_retry_kind(ac.retry_kind),
                    scheduled_at=next_fires[1],
                    payload_schema_ver=1,
                    identity_key=schedule_identity_key,
                )
                await backend.enqueue_with_conn(conn, overlap_enqueue)
                log.info(
                    "cron dst overlap second fire",
                    kind="cron_fire",
                    actor=actor,
                    schedule_id=str(row["id"]),
                )
            await conn.execute(
                f'UPDATE "{schema}".cron_schedules '
                f"SET last_fired_at = now(), last_fire_error = NULL, "
                f"consecutive_failures = 0, next_fire_at = $2 "
                f"WHERE id = $1",
                row["id"],
                next_fire,
            )

            published_queue = ac.queue

            prev_consecutive: int = row["consecutive_failures"] or 0
            if prev_consecutive > 0:
                record_cron_failure(str(row["id"]), -prev_consecutive)

            log.info(
                "cron fired",
                kind="cron_fire",
                actor=actor,
                schedule_id=str(row["id"]),
                next_fire_at=next_fire.isoformat(),
            )
        except Exception as exc:
            span.set_status(StatusCode.ERROR, str(exc))
            consecutive: int = (row["consecutive_failures"] or 0) + 1
            auto_disable = consecutive >= settings.cron_auto_disable_threshold

            if auto_disable:
                span.add_event(
                    "cron.auto_disabled",
                    {
                        "schedule_name": row["actor"],
                        "last_error": str(exc),
                        "failure_count": consecutive,
                    },
                )

                status = await conn.execute(
                    f'UPDATE "{schema}".cron_schedules '
                    f"SET last_fire_error = $2, consecutive_failures = $3, "
                    f"enabled = false WHERE id = $1 AND enabled = true",
                    row["id"],
                    str(exc),
                    consecutive,
                )
                if status == "UPDATE 0":
                    log.warning(
                        "cron auto-disable skipped; schedule re-enabled before UPDATE",
                        kind="cron_fire",
                        schedule_id=str(row["id"]),
                    )

                disabled_count: int = await conn.fetchval(
                    f'SELECT COUNT(*) FROM "{schema}".cron_schedules WHERE enabled = false'
                )
                update_disabled_schedules_count(disabled_count)

                log.error(
                    "cron schedule auto-disabled",
                    kind="cron_fire",
                    actor=row["actor"],
                    schedule_id=str(row["id"]),
                    consecutive_failures=consecutive,
                    error=str(exc),
                )
            else:
                status = await conn.execute(
                    f'UPDATE "{schema}".cron_schedules '
                    f"SET last_fire_error = $2, consecutive_failures = $3 "
                    f"WHERE id = $1 AND enabled = true",
                    row["id"],
                    str(exc),
                    consecutive,
                )
                if status == "UPDATE 0":
                    log.warning(
                        "cron error UPDATE skipped; schedule no longer enabled",
                        kind="cron_fire",
                        schedule_id=str(row["id"]),
                    )

                log.error(
                    "cron fire failed",
                    kind="cron_fire",
                    actor=row["actor"],
                    schedule_id=str(row["id"]),
                    consecutive_failures=consecutive,
                    error=str(exc),
                )

            record_cron_failure(str(row["id"]), 1)

    if published_queue is not None:
        record_published_message(row["actor"], published_queue)

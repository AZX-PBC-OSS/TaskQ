"""Worker bootstrap utilities: config sync, startup sequencing, and pre-flight checks."""

from collections.abc import Sequence

import asyncpg
import structlog

from taskq._json import dumps_str, loads
from taskq.backend._protocol import ConnLike
from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: reusing the canonical identifier regex rather than redefining it
)
from taskq.exceptions import ActorConfigDriftError, ActorConfigDriftList
from taskq.obs import get_logger
from taskq.worker.actor_config import ActorConfig

logger: structlog.stdlib.BoundLogger = get_logger(__name__)

_SELECT_ACTOR_CONFIG_SQL = """
SELECT actor, max_concurrent, max_pending, queue, result_ttl, metadata
  FROM "{schema}".actor_config
 WHERE actor = ANY($1::text[])
""".strip()

_UPSERT_ACTOR_CONFIG_SQL = """
INSERT INTO "{schema}".actor_config (actor, max_concurrent, max_pending, queue, result_ttl, metadata)
SELECT actor, max_concurrent, max_pending, queue, result_ttl, metadata::jsonb
  FROM unnest(
      $1::text[], $2::int[], $3::int[], $4::text[], $5::float[], $6::text[]
  ) AS t(actor, max_concurrent, max_pending, queue, result_ttl, metadata)
ON CONFLICT (actor) DO UPDATE SET
    max_concurrent = EXCLUDED.max_concurrent,
    max_pending    = EXCLUDED.max_pending,
    queue          = EXCLUDED.queue,
    result_ttl     = EXCLUDED.result_ttl,
    metadata       = EXCLUDED.metadata,
    updated_at     = now()
""".strip()


async def sync_actor_config(
    conn: ConnLike,
    actor_configs: Sequence[ActorConfig],
    *,
    force: bool = False,
    schema: str = "taskq",
) -> None:
    """Populate `{schema}.actor_config` rows at worker startup.

    Two-phase write:
      1. SELECT existing rows for the registered actors.
      2. For each registered actor whose stored row differs from the
         registered values, collect one ``ActorConfigDriftError`` per
         differing field. If ``force=False``
         and one or more drifts exist, raise ``ActorConfigDriftList``
         containing all collected drifts. If ``force=True``, log
         ``actor-config-drift-overwrite`` at error level for each drift
         and continue.
      3. Upsert all registered rows via ``INSERT ... ON CONFLICT (actor)
         DO UPDATE SET max_concurrent = EXCLUDED.max_concurrent,
         queue = EXCLUDED.queue, metadata = EXCLUDED.metadata,
         updated_at = now()``.

    Both phases run inside a single ``async with conn.transaction():``
    block so a SELECT-then-UPSERT race is impossible against another
    worker's startup. The empty-actor-list case is a no-op.
    """
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")

    count = len(actor_configs)
    logger.info(
        "actor-config-sync-start",
        count=count,
        force=force,
    )

    if count == 0:
        return

    async with conn.transaction():
        actor_names = [cfg.actor for cfg in actor_configs]

        rows = await conn.fetch(
            _SELECT_ACTOR_CONFIG_SQL.format(schema=schema),
            actor_names,
        )

        stored: dict[str, asyncpg.Record] = {}
        for row in rows:
            stored[row["actor"]] = row

        drifts: list[ActorConfigDriftError] = []

        for cfg in actor_configs:
            stored_row = stored.get(cfg.actor)
            if stored_row is None:
                continue

            stored_mc = stored_row["max_concurrent"]
            stored_mp = stored_row["max_pending"]
            stored_queue = stored_row["queue"]
            stored_result_ttl = stored_row["result_ttl"]
            stored_metadata_raw: str = stored_row["metadata"]
            stored_metadata: dict[str, object] = loads(stored_metadata_raw)

            if cfg.max_concurrent != stored_mc:
                drift = ActorConfigDriftError(
                    actor=cfg.actor,
                    field="max_concurrent",
                    registered=cfg.max_concurrent,
                    stored=stored_mc,
                )
                drifts.append(drift)
                logger.error(
                    "actor-config-drift-overwrite",
                    actor=cfg.actor,
                    field="max_concurrent",
                    registered=cfg.max_concurrent,
                    stored=stored_mc,
                    force=force,
                )

            if cfg.max_pending != stored_mp:
                drift = ActorConfigDriftError(
                    actor=cfg.actor,
                    field="max_pending",
                    registered=cfg.max_pending,
                    stored=stored_mp,
                )
                drifts.append(drift)
                logger.error(
                    "actor-config-drift-overwrite",
                    actor=cfg.actor,
                    field="max_pending",
                    registered=cfg.max_pending,
                    stored=stored_mp,
                    force=force,
                )

            if cfg.queue != stored_queue:
                drift = ActorConfigDriftError(
                    actor=cfg.actor,
                    field="queue",
                    registered=cfg.queue,
                    stored=stored_queue,
                )
                drifts.append(drift)
                logger.error(
                    "actor-config-drift-overwrite",
                    actor=cfg.actor,
                    field="queue",
                    registered=cfg.queue,
                    stored=stored_queue,
                    force=force,
                )

            if cfg.result_ttl != stored_result_ttl:
                drift = ActorConfigDriftError(
                    actor=cfg.actor,
                    field="result_ttl",
                    registered=cfg.result_ttl,
                    stored=stored_result_ttl,
                )
                drifts.append(drift)
                logger.error(
                    "actor-config-drift-overwrite",
                    actor=cfg.actor,
                    field="result_ttl",
                    registered=cfg.result_ttl,
                    stored=stored_result_ttl,
                    force=force,
                )

            if cfg.metadata != stored_metadata:
                drift = ActorConfigDriftError(
                    actor=cfg.actor,
                    field="metadata",
                    registered=cfg.metadata,
                    stored=stored_metadata,
                )
                drifts.append(drift)
                logger.error(
                    "actor-config-drift-overwrite",
                    actor=cfg.actor,
                    field="metadata",
                    registered=cfg.metadata,
                    stored=stored_metadata,
                    force=force,
                )

        if drifts and not force:
            raise ActorConfigDriftList(tuple(drifts))

        mc_array: list[int | None] = [cfg.max_concurrent for cfg in actor_configs]
        mp_array: list[int | None] = [cfg.max_pending for cfg in actor_configs]
        queue_array: list[str] = [cfg.queue for cfg in actor_configs]
        result_ttl_array: list[float | None] = [cfg.result_ttl for cfg in actor_configs]
        metadata_array: list[str] = [dumps_str(cfg.metadata) for cfg in actor_configs]

        await conn.execute(
            _UPSERT_ACTOR_CONFIG_SQL.format(schema=schema),
            actor_names,
            mc_array,
            mp_array,
            queue_array,
            result_ttl_array,
            metadata_array,
        )

    logger.info(
        "actor-config-synced",
        total_count=count,
    )

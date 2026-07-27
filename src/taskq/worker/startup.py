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

# ``max_concurrent``, ``max_pending``, and ``result_ttl`` are deliberately
# absent from the ``DO UPDATE SET`` clause: Postgres leaves an unlisted
# column at its current value on conflict, so an existing row's capacity
# fields survive every subsequent startup untouched no matter what the
# ``@actor(...)`` literal says. Those columns are only ever populated via
# the ``INSERT`` list — i.e. the first time a row is created (seeding) — or
# via `taskq actor-config set` (operator override). ``queue`` and
# ``metadata`` remain structural: they are always re-written from the
# registered value, which is safe because `sync_actor_config` has already
# raised (or the caller passed ``force=True``) for any structural drift
# before this statement runs.
_UPSERT_ACTOR_CONFIG_SQL = """
INSERT INTO "{schema}".actor_config (actor, max_concurrent, max_pending, queue, result_ttl, metadata)
SELECT actor, max_concurrent, max_pending, queue, result_ttl, metadata::jsonb
  FROM unnest(
      $1::text[], $2::int[], $3::int[], $4::text[], $5::float[], $6::text[]
  ) AS t(actor, max_concurrent, max_pending, queue, result_ttl, metadata)
ON CONFLICT (actor) DO UPDATE SET
    queue          = EXCLUDED.queue,
    metadata       = EXCLUDED.metadata,
    updated_at     = now()
""".strip()

# Fields whose stored value is operator-owned once a row exists: the
# ``@actor(...)`` literal only seeds the row on first registration
# (`stored_row is None` branch below); on every subsequent startup the
# stored value wins and a differing literal is *expected*, not an error.
_CAPACITY_FIELDS = ("max_concurrent", "max_pending", "result_ttl")

# Fields where a stored/registered mismatch indicates a real correctness
# bug (e.g. a stale pod routing an actor at the wrong queue) rather than a
# deliberate operator override, and therefore still raise unless
# ``force=True``.
_STRUCTURAL_FIELDS = ("queue", "metadata")


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
      2. For each registered actor with a stored row, compare the
         registered value to the stored value field by field:

         - **Capacity fields** (``max_concurrent``, ``max_pending``,
           ``result_ttl``) are operator-owned once a row exists. A
           differing registered literal is logged at
           ``actor-config-capacity-override`` (info level — this is an
           expected operator override, not a bug) and never raises. The
           stored value is left untouched by the UPSERT below.
         - **Structural fields** (``queue``, ``metadata``) still raise:
           one ``ActorConfigDriftError`` per differing field, collected
           into ``ActorConfigDriftList`` and raised unless ``force=True``.
           With ``force=True`` the mismatch is logged at
           ``actor-config-drift-overwrite`` (error level) and the UPSERT
           overwrites the stored value.
      3. Upsert all registered rows via ``INSERT ... ON CONFLICT (actor)
         DO UPDATE SET queue = EXCLUDED.queue, metadata =
         EXCLUDED.metadata, updated_at = now()`` — capacity columns are
         omitted from the ``SET`` clause so an existing row's
         ``max_concurrent`` / ``max_pending`` / ``result_ttl`` survive
         unchanged; they are populated by the ``INSERT`` list only when
         the row is first created.

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

            stored_metadata: dict[str, object] = loads(stored_row["metadata"])

            capacity_values: dict[str, int | float | None] = {
                "max_concurrent": stored_row["max_concurrent"],
                "max_pending": stored_row["max_pending"],
                "result_ttl": stored_row["result_ttl"],
            }
            for field in _CAPACITY_FIELDS:
                registered_value = getattr(cfg, field)
                stored_value = capacity_values[field]
                if registered_value != stored_value:
                    logger.info(
                        "actor-config-capacity-override",
                        actor=cfg.actor,
                        field=field,
                        registered=registered_value,
                        stored=stored_value,
                    )

            structural_values: dict[str, str | dict[str, object]] = {
                "queue": stored_row["queue"],
                "metadata": stored_metadata,
            }
            for field in _STRUCTURAL_FIELDS:
                registered_value = getattr(cfg, field)
                stored_value = structural_values[field]
                if registered_value != stored_value:
                    drift = ActorConfigDriftError(
                        actor=cfg.actor,
                        field=field,  # type: ignore[arg-type]  # Why: field iterates over _STRUCTURAL_FIELDS, a subset of ActorConfigDriftError's Literal; pyright cannot narrow str -> Literal across the loop variable.
                        registered=registered_value,
                        stored=stored_value,
                    )
                    drifts.append(drift)
                    if force:
                        logger.error(
                            "actor-config-drift-overwrite",
                            actor=cfg.actor,
                            field=field,
                            registered=registered_value,
                            stored=stored_value,
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

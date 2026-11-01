"""Worker bootstrap utilities: config sync, startup sequencing, and pre-flight checks."""

from collections.abc import Sequence
from datetime import timedelta
from typing import Final

import asyncpg
import structlog

from taskq._json import dumps_jsonb_str, loads
from taskq.actor_config import CAPACITY_OWNER_CODE, ActorConfig
from taskq.backend._protocol import ConnLike
from taskq.constants import (
    require_schema,
)
from taskq.exceptions import ActorConfigDriftError, ActorConfigDriftList
from taskq.obs import get_logger

logger: structlog.stdlib.BoundLogger = get_logger(__name__)

_SELECT_ACTOR_CONFIG_SQL = """
SELECT actor, max_concurrent, max_pending, queue, result_ttl, metadata,
       max_attempts, retry_kind
  FROM "{schema}".actor_config
 WHERE actor = ANY($1::text[])
""".strip()

# ── The upserts' shared body ────────────────────────────────────────────
# ONE INSERT…SELECT…unnest statement (the 12-position bind list included)
# with a ``{capacity_set}`` slot for the ``DO UPDATE SET`` clause's
# capacity assignments — the two faces below render the same body, so
# their bind lists cannot drift. The slot is filled by token replacement
# (the house convention, never ``.format``): the body keeps ``{schema}``
# for the caller's ``.format(schema=schema)`` at the boot-time execute.
_ACTOR_CONFIG_UPSERT_BODY_SQL = """
INSERT INTO "{schema}".actor_config (
    actor, max_concurrent, max_pending, queue, result_ttl, metadata,
    retry_base, retry_cap, retry_backoff, retry_jitter,
    max_attempts, retry_kind
)
SELECT actor, max_concurrent, max_pending, queue, result_ttl, metadata::jsonb,
       retry_base, retry_cap, retry_backoff, retry_jitter,
       max_attempts, retry_kind
  FROM unnest(
      $1::text[], $2::int[], $3::int[], $4::text[], $5::float[], $6::text[],
      $7::interval[], $8::interval[], $9::text[], $10::float8[],
      $11::smallint[], $12::text[]
  ) AS t(actor, max_concurrent, max_pending, queue, result_ttl, metadata,
         retry_base, retry_cap, retry_backoff, retry_jitter,
         max_attempts, retry_kind)
ON CONFLICT (actor) DO UPDATE SET
    metadata       = EXCLUDED.metadata,
{capacity_set}    max_attempts   = EXCLUDED.max_attempts,
    retry_kind     = EXCLUDED.retry_kind,
    updated_at     = clock_timestamp()
"""

# ``max_concurrent``, ``max_pending``, ``result_ttl``, and ``queue`` are
# deliberately absent from the ``DO UPDATE SET`` clause: Postgres leaves an
# unlisted column at its current value on conflict, so an existing row's
# capacity fields and queue assignment survive every subsequent startup
# untouched no matter what the ``@actor(...)`` literal says. The capacity
# columns are only ever populated via the ``INSERT`` list, i.e. the first
# time a row is created (seeding), or via `taskq actor-config set`
# (operator override); the queue assignment via the ``INSERT`` list or
# `taskq actor-config move-queue` (the one-step operator move). Keeping the
# assignment out of the conflict clause is what makes a move durable across
# a rolling deploy: a worker still carrying the old literal boots, logs
# ``actor-config-queue-override``, and cannot flip the row back.
# ``metadata`` remains structural: it is always re-written from the
# registered value, which is safe because `sync_actor_config` has already
# raised (or the caller passed ``force=True``) for metadata drift before
# this statement runs.
# ``max_attempts`` and ``retry_kind`` are the opposite family: CODE-owned
# on every boot, always re-written from the ``@actor(...)`` literal. They
# decide how many attempts a server-side fire gets and which retry family
# it belongs to (cron fires and the admin run-now build their EnqueueArgs
# from the stored row), so leaving them at the first-registration value
# hands every later fire a stale retry contract. No operator surface can
# write these columns (`taskq actor-config set` moves capacity, `move-queue`
# moves the queue, neither touches them), so a stored/registered
# disagreement can only be a changed code literal or an out-of-band hand
# edit; the code literal is the declaration of record and must win, and
# ``actor-config-retry-contract-change`` (emitted by `sync_actor_config`
# below) is what makes the win visible instead of silent.
#: The two faces' ``{capacity_set}`` slot fills: the operator face
#: assigns nothing beyond the shared body's own SET clause; the
#: code-owned face adds exactly the two capacity assignments (the
#: module's own render inputs — the smoke guard's ``_render_extra``
#: resolves the slot from here, never a copy).
_OPERATOR_CAPACITY_SET: Final[str] = ""
_CODE_OWNED_CAPACITY_SET: Final[str] = (
    "    max_concurrent = EXCLUDED.max_concurrent,\n    result_ttl     = EXCLUDED.result_ttl,\n"
)

_UPSERT_ACTOR_CONFIG_SQL = _ACTOR_CONFIG_UPSERT_BODY_SQL.replace(
    "{capacity_set}", _OPERATOR_CAPACITY_SET
).strip()

# THE CODE-OWNED CAPACITY UPSERT (C15's opt-in): the SAME statement as
# ``_UPSERT_ACTOR_CONFIG_SQL`` except the ``DO UPDATE SET`` clause also
# rewrites ``max_concurrent`` and ``result_ttl`` from the declared
# literal — the retry contract's own law, on the capacity axes. It runs
# ONLY for the actors that opted in (``@actor(capacity_owner="code")`` /
# ``@app.actor(capacity_owner="code")``): a changed literal deploys by
# code on every boot instead of silently doing nothing after first boot.
# The default rows never touch this statement — the operator statement
# above is byte-identical for them (the operator-owns law holds).
#
# Deliberately still ABSENT from the SET clause, for both owners:
# * ``queue`` — the assignment is the operator's rolling move
#   (`taskq actor-config move-queue`): old-literal workers boot beside
#   the move and a code literal must not flip the row back (the
#   ``actor-config-queue-override`` warning's own subject). The
#   capacity_owner declaration moves WHO owns capacity, not WHO owns
#   placement.
# * ``max_pending`` — its enforcement is the PRODUCER-side enqueue
#   admission, which reads the STORED row through a TTL-bounded cache;
#   the backlog cap is the operator's lever on the producers exactly
#   like the queue is (``taskq actor-config set``), and a code flip
#   would retune live admission out from under producers still holding
#   the cached cap. The claim-side cannot bound a backlog (claiming is
#   the only drain — the H-A6 law), so the stored row is the only
#   enforcement seat there is.
_UPSERT_CODE_OWNED_CAPACITY_SQL = _ACTOR_CONFIG_UPSERT_BODY_SQL.replace(
    "{capacity_set}", _CODE_OWNED_CAPACITY_SET
).strip()

# Fields whose stored value is operator-owned once a row exists: the
# ``@actor(...)`` literal only seeds the row on first registration
# (`stored_row is None` branch below); on every subsequent startup the
# stored value wins and a differing literal is *expected*, not an error.
# The queue assignment belongs to this family too, moved by
# `taskq actor-config move-queue`, never by a boot, but is surfaced at
# WARNING (`actor-config-queue-override`) rather than info, because cron
# fires follow the stored queue while producers enqueue by their own
# literal: the disagreement is real drift to surface. The check is written
# out explicitly below rather than joined to this tuple so that
# difference stays visible at the comparison site.
_CAPACITY_FIELDS = ("max_concurrent", "max_pending", "result_ttl")

# Fields where a stored/registered mismatch indicates a real correctness
# bug rather than a deliberate operator override, no operator surface can
# move them, so any mismatch is one, and therefore still raises unless
# ``force=True``.
_STRUCTURAL_FIELDS = ("metadata",)

# The capacity fields a ``capacity_owner="code"`` actor hands to the
# declaration: the code-owned upsert's SET clause rewrites exactly these
# from the literal on every boot. ``max_pending`` is deliberately not
# here (see ``_UPSERT_CODE_OWNED_CAPACITY_SQL`` — the producer-side
# admission reads the stored row; the operator keeps the backlog cap),
# and so is the queue (the move-queue law, placement is never code-owned).
_CODE_OWNED_CAPACITY_FIELDS = ("max_concurrent", "result_ttl")


def capacity_field_diverges(registered_value: object, stored_value: object) -> bool:
    """Whether a capacity field's ``@actor(...)`` literal disagrees with its
    stored ``actor_config`` value, the single predicate every capacity
    -divergence surface in the codebase shares (`sync_actor_config`'s
    ``actor-config-capacity-override`` event below, and the boot-time
    ``actor-config-capacity-divergence`` line in ``worker/run.py``).

    Plain inequality: unlike ``max_pending``/``result_ttl`` (which fall
    back to the literal when the stored value is ``NULL``), the
    comparison itself is symmetric, a literal of ``None`` (uncapped)
    against a stored numeric cap is exactly as much a divergence as the
    reverse, so no side gets an early-exit guard that the other lacks.
    """
    return registered_value != stored_value


async def read_stored_queue_assignments(
    conn: ConnLike,
    actors: Sequence[str],
    *,
    schema: str = "taskq",
) -> dict[str, str]:
    """Return ``{actor: queue}`` for the stored ``actor_config`` rows.

    The stored assignment, not the ``@actor(queue=...)`` literal, is what
    routes a job: the cron leader fires onto it and every re-pend follows
    it, and `taskq actor-config move-queue` rewrites it without touching
    any code. Actors with no row yet are absent from the mapping, their
    literal is what the first sync will seed.

    Never raises on a connection that cannot answer schema questions: the
    boot path's duck-typed pool stubs return no rows, which degrades to
    "no stored assignments known" rather than a boot failure, matching
    the pending-migration guard's own degradation convention.
    """
    require_schema(schema)
    if not actors:
        return {}
    rows = await conn.fetch(_SELECT_ACTOR_CONFIG_SQL.format(schema=schema), list(actors))
    return {row["actor"]: row["queue"] for row in rows}


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
           ``actor-config-capacity-override`` (info level, this is an
           expected operator override, not a bug) and never raises. The
           stored value is left untouched by the UPSERT below.
         - **The queue assignment** is likewise operator-owned once a row
           exists (moved by `taskq actor-config move-queue`): a differing
           literal is logged at ``actor-config-queue-override`` (warning
           level, cron fires follow the stored queue while producers
           enqueue by their own literal, so the disagreement is real drift
           to surface) and never raises. This is the rolling-deploy window
           of a queue move: old-literal and new-literal workers both boot,
           and the UPSERT below preserves the stored assignment so a stale
           literal cannot undo the move.
         - **Metadata** still raises: one ``ActorConfigDriftError`` per
           differing field, collected into ``ActorConfigDriftList`` and
           raised unless ``force=True``. With ``force=True`` the mismatch
           is logged at ``actor-config-drift-overwrite`` (error level) and
           the UPSERT overwrites the stored value.
         - **The retry contract** (``max_attempts``, ``retry_kind``) is
           code-owned: a differing registered literal is logged at
           ``actor-config-retry-contract-change`` (warning level) and the
           UPSERT below overwrites the stored pair with the registered
           value. These columns decide the retry behaviour of every
           server-side fire (cron fires and the admin run-now build their
           ``EnqueueArgs`` from the stored row), so the declaration of
           record must win on every boot, and no operator surface can
           write them.
         - **A ``capacity_owner="code"`` actor's** ``max_concurrent`` and
           ``result_ttl`` join the retry contract's family (C15's
           opt-in): a differing registered literal is logged at
           ``actor-config-capacity-code-change`` (warning level) and the
           UPSERT overwrites the stored pair with the registered value —
           a changed literal deploys by code on every boot instead of
           silently doing nothing after first boot. The queue and
           ``max_pending`` stay operator-owned for BOTH owners (the
           move-queue law; the producer-side enqueue admission reads the
           stored row).
      3. Upsert all registered rows via ``INSERT ... ON CONFLICT (actor)
         DO UPDATE SET metadata = EXCLUDED.metadata, max_attempts =
         EXCLUDED.max_attempts, retry_kind = EXCLUDED.retry_kind,
         updated_at = clock_timestamp()``: the capacity columns and the
         queue assignment are omitted from the ``SET`` clause so an
         existing row's ``max_concurrent`` / ``max_pending`` /
         ``result_ttl`` / ``queue`` survive unchanged; they are populated
         by the ``INSERT`` list only when the row is first created. The
         retry contract columns are in the ``SET`` clause so a changed
         ``@actor`` literal reaches the next server-side fire. Actors
         that declared ``capacity_owner="code"`` ride a SECOND statement
         (same transaction, disjoint actors) whose ``SET`` clause adds
         ``max_concurrent = EXCLUDED.max_concurrent, result_ttl =
         EXCLUDED.result_ttl`` — the C15 opt-in; with no code-owned
         actors the operator statement alone runs, byte-identical to the
         pre-opt-in shape.

    Both phases run inside a single ``async with conn.transaction():``
    block so a SELECT-then-UPSERT race is impossible against another
    worker's startup. The empty-actor-list case is a no-op.
    """
    require_schema(schema)

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
                if capacity_field_diverges(registered_value, stored_value):
                    if (
                        cfg.capacity_owner == CAPACITY_OWNER_CODE
                        and field in _CODE_OWNED_CAPACITY_FIELDS
                    ):
                        # THE CODE DEPLOY IS VISIBLE (C15): the code-owned
                        # actor's literal is about to WIN — the upsert's
                        # SET clause below rewrites this field from the
                        # declaration. Warning, never silent: the stored
                        # capacity is about to change by code, the same
                        # visibility the retry contract's own change
                        # event carries.
                        logger.warning(
                            "actor-config-capacity-code-change",
                            actor=cfg.actor,
                            field=field,
                            registered=registered_value,
                            stored=stored_value,
                        )
                    else:
                        logger.info(
                            "actor-config-capacity-override",
                            actor=cfg.actor,
                            field=field,
                            registered=registered_value,
                            stored=stored_value,
                        )

            if cfg.queue != stored_row["queue"]:
                # Warning, never an error: this is either the rolling-deploy
                # window of `taskq actor-config move-queue` (stored row
                # moved, this process's literal not yet redeployed, or the
                # reverse) or a literal that has not followed the fleet's
                # assignment. The stored queue routes the cron leader's
                # fires; producers enqueue by their own literal, so the
                # disagreement is real drift to surface, but refusing boot
                # here is exactly what made a queue move need lockstep
                # coordination. The newest assignment (the stored row)
                # wins, and the UPSERT below preserves it.
                logger.warning(
                    "actor-config-queue-override",
                    actor=cfg.actor,
                    registered=cfg.queue,
                    stored=stored_row["queue"],
                )

            retry_contract_values: dict[str, object] = {
                "max_attempts": stored_row["max_attempts"],
                "retry_kind": stored_row["retry_kind"],
            }
            registered_retry_contract: dict[str, object] = {
                "max_attempts": cfg.max_attempts,
                "retry_kind": cfg.retry_kind,
            }
            if retry_contract_values != registered_retry_contract:
                # Warning, never an error: max_attempts/retry_kind are
                # code-owned and the UPSERT below overwrites the stored
                # pair with the registered literal on every boot. No
                # operator surface can write these columns, so the stored
                # value differing means either the code literal changed
                # since the last registration or the row was edited
                # out-of-band; either way the stored retry contract is
                # about to change, and server-side fires (cron, admin
                # run-now) read this row, so the change is worth a line
                # in the log even though it is not drift to refuse.
                logger.warning(
                    "actor-config-retry-contract-change",
                    actor=cfg.actor,
                    registered=registered_retry_contract,
                    stored=retry_contract_values,
                )

            structural_values: dict[str, dict[str, object]] = {
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

        async def _upsert(statements: list[tuple[str, list[ActorConfig]]]) -> None:
            """One upsert execute per (statement, rows) pair, IN ORDER:
            the operator rows ride the operator statement, the
            code-owned rows ride the code-owned one (whose SET clause
            adds the capacity columns). Same transaction, disjoint
            actors (the partition's own key), so the order is inert."""
            for sql, configs in statements:
                mc_array: list[int | None] = [cfg.max_concurrent for cfg in configs]
                mp_array: list[int | None] = [cfg.max_pending for cfg in configs]
                queue_array: list[str] = [cfg.queue for cfg in configs]
                result_ttl_array: list[float | None] = [cfg.result_ttl for cfg in configs]
                metadata_array: list[str] = [dumps_jsonb_str(cfg.metadata) for cfg in configs]
                # The declared retry curve: seeded on first create, so
                # cron fires and the admin run-now re-pend on the curve
                # the actor declared instead of the EnqueueArgs defaults.
                base_array: list[timedelta | None] = [cfg.retry_base for cfg in configs]
                cap_array: list[timedelta | None] = [cfg.retry_cap for cfg in configs]
                backoff_array: list[str | None] = [cfg.retry_backoff for cfg in configs]
                jitter_array: list[float | None] = [cfg.retry_jitter for cfg in configs]
                # The declared retry contract: code-owned, rewritten on
                # every boot (see the upsert SQL comment for why this
                # family is not operator-owned like the capacity fields).
                attempts_array: list[int] = [cfg.max_attempts for cfg in configs]
                kind_array: list[str] = [cfg.retry_kind for cfg in configs]
                names_array: list[str] = [cfg.actor for cfg in configs]
                run_arrays: tuple[object, ...] = (
                    names_array,
                    mc_array,
                    mp_array,
                    queue_array,
                    result_ttl_array,
                    metadata_array,
                    base_array,
                    cap_array,
                    backoff_array,
                    jitter_array,
                    attempts_array,
                    kind_array,
                )
                await conn.execute(sql.format(schema=schema), *run_arrays)

        operator_configs = [
            cfg for cfg in actor_configs if cfg.capacity_owner != CAPACITY_OWNER_CODE
        ]
        code_owned_configs = [
            cfg for cfg in actor_configs if cfg.capacity_owner == CAPACITY_OWNER_CODE
        ]
        # THE DEFAULT PATH IS BYTE-IDENTICAL: no code-owned rows → the
        # operator statement alone, exactly as before this opt-in
        # existed (same SQL text, same arrays, same single execute).
        statements: list[tuple[str, list[ActorConfig]]] = [
            (_UPSERT_ACTOR_CONFIG_SQL, operator_configs)
        ]
        if code_owned_configs:
            statements.append((_UPSERT_CODE_OWNED_CAPACITY_SQL, code_owned_configs))
        await _upsert(statements)

    logger.info(
        "actor-config-synced",
        total_count=count,
    )

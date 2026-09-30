"""The retention-policy floor probe, in its own asyncpg-free module.

This helper lives outside :mod:`taskq.timescale` for a contract reason:
:mod:`taskq.timescale` is the deploy-step conversion machinery
(module-level ``import asyncpg``), while the sweeps bind the floor probe
by name — ``taskq.backend._sweeps`` and ``taskq.worker._leader_shared``
both import it — and ``taskq.testing``'s sweeps twin imports the real
sweeps module as its parity seam.  The chain
``taskq.testing → backend/_sweeps → taskq.timescale → asyncpg`` would drag
asyncpg into ``sys.modules`` on ``import taskq.testing``, breaking the
no-transitive-heavy-deps contract
(``tests/test_memory_jobs_fixture.py::test_testing_no_transitive_asyncpg``).

The probe needs none of the conversion machinery: it is ONE catalog
query against a connection the CALLER owns and hands in, so it lives
here with NO module-level asyncpg import — the connection parameter is
typed through ``TYPE_CHECKING`` plus the future-annotations string
forms, and the runtime imports are stdlib-only.  Deploy-step code that
wants the probe imports it from here; :mod:`taskq.timescale` stays
clean of this module (its enable/disable paths never call the floor —
the sweeps do).

The floor itself: is *schema.table* a hypertable whose aged end is
owned by a registered, SCHEDULED TimescaleDB retention policy, and where
does that ownership begin — ``now - drop_after``, parsed from the
policy's OWN registered config, never re-derived from TaskQ's settings.
(A paused job — ``alter_job(job_id, scheduled => FALSE)`` — is
registered but never runs, so it owns nothing: see the None cases on
:func:`retention_policy_floor`.)
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from taskq.backend._protocol import ConnLike

__all__ = ["retention_policy_floor"]

logger = logging.getLogger(__name__)

# One round trip answering both probe questions: is *schema.table* a
# hypertable with a registered retention policy (the information views'
# join), and if so what is the policy's own horizon.  The policy's
# ``drop_after`` is read out of the registered job's ``config`` — the
# EXACT interval the policy drops on, not a re-derivation from TaskQ's
# settings, so a policy registered by any release with any interval is
# honored as registered.  ``timescaledb_information.dimensions`` pins the
# hypertable's time column to the caller's *partition_col*: the floor's
# clock IS the partition column's, and a caller passing a column the
# hypertable is not partitioned on gets None (fail-open) rather than a
# floor drawn on the wrong clock.  Every value is $-bound — nothing here
# is interpolated.  On vanilla Postgres the views do not exist and the
# statement raises UndefinedTable: caught below, the fail-open.
_RETENTION_POLICY_FLOOR_PROBE_SQL = """\
SELECT MIN(((j.config)::text::jsonb ->> 'drop_after')::interval) AS drop_after,
       MIN(statement_timestamp()) AS db_now
FROM timescaledb_information.hypertables h
JOIN timescaledb_information.jobs j
  ON j.hypertable_schema = h.hypertable_schema
 AND j.hypertable_name = h.hypertable_name
WHERE h.hypertable_schema = $1
  AND h.hypertable_name = $2
  AND j.proc_name = 'policy_retention'
  AND j.scheduled
  AND EXISTS (
      SELECT 1 FROM timescaledb_information.dimensions d
      WHERE d.hypertable_schema = $1
        AND d.hypertable_name = $2
        AND d.column_name = $3
        AND d.dimension_type = 'Time'
  )
-- Aggregates (MIN), not LIMIT 1 without ORDER BY: the single-row answer is
-- deterministic no matter the catalog's physical order, and the no-policy
-- case still returns exactly one row of NULLs (the caller's None).
-- ``j.scheduled`` is the ownership claim's teeth: a PAUSED policy job
-- (``alter_job(job_id, scheduled => FALSE)``) is registered but never
-- runs, so deferring the sweep's aged end to it strands those rows with
-- nobody deleting them (the downgrade strand's compound, reachable on a
-- healthy TSL server by pausing alone). A paused job reads as "no
-- answer" - the caller's None, the sweep full-range - same polarity as
-- the no-job case."""


async def retention_policy_floor(
    conn: ConnLike,
    schema: str,
    table: str,
    partition_col: str,
    now: datetime | None = None,
) -> datetime | None:
    """The armed retention policy's own horizon for one hypertable, or
    None when nothing owns the aged end.

    Answers ONE catalog question per sweep run (never per batch): is
    *schema.table* a hypertable with a ``policy_retention`` job registered
    against it, and if so, when does that policy's ownership of the aged
    end begin?  The answer is ``now - drop_after``, where *drop_after* is
    parsed out of the registered policy's own config
    (``timescaledb_information.jobs.config``) — the policy's EXACT
    horizon, not a re-derivation from TaskQ's settings, so the floor and
    the policy can never disagree about where the boundary sits.

    *partition_col* is the table's partition column (``occurred_at`` for
    ``job_events``, ``finished_at`` for ``jobs_archive``): the policy
    drops chunks on that column's age, and the probe verifies through
    ``timescaledb_information.dimensions`` that the hypertable really is
    partitioned on it — a mismatch returns None (the caller's floor
    semantics would be drawn on the wrong clock).

    Returns None — and the caller's sweep then runs full-range, byte-
    identical to vanilla behavior — when ANY of:

    * the table is not a hypertable (no row in
      ``timescaledb_information.hypertables``),
    * the hypertable carries no ``policy_retention`` job (nothing owns
      the aged end; the sweep must not skip a single row),
    * the hypertable's ``policy_retention`` job is registered but PAUSED
      (``scheduled = false`` — ``alter_job``'s supported knob): a policy
      that never runs owns nothing, and deferring to it would strand the
      aged end with nobody deleting it,
    * the hypertable is partitioned on a column other than
      *partition_col*,
    * the probe or the ROW PARSE fails for ANY reason — vanilla Postgres
      above all (the ``timescaledb_information`` views do not exist there
      and the statement raises on first execution), but also permission
      gaps, extension upgrades, config shapes the cast cannot parse, and
      connection wrappers whose fetched rows are not keyed the way the
      real driver's are (a row without the ``drop_after``/``db_now`` keys
      must read as "no answer", never as a crash).

    That vanilla UndefinedTable is BY DESIGN the smoke guard's one plan
    carve-out: ``tests/test_sql_templates_smoke_pg.py`` keeps its parse
    teeth on this probe but excuses it from the PLAN demand vanilla cannot
    meet (``_PLAN_ON_VANILLA_BY_DESIGN``, keyed on the statement's own
    ``timescaledb_information.hypertables`` marker) — the probe's shape,
    not a defect.

    A probe or parse failure must never break a sweep: the failure is
    logged at debug and the sweep keeps today's exact behavior.  *now*
    overrides the clock the floor is anchored to (test seam); by default
    the probe query's own ``statement_timestamp()`` (read in the same
    round trip) anchors it to the database's clock domain, the domain
    every retention predicate in the sweeps runs in.

    The floor is a BOUNDARY, not a deletion order: rows NEWER than ``now -
    drop_after`` (inside the window) the calling sweep keeps deleting row-
    exactly; rows OLDER than it the registered policy owns — silently, at
    chunk granularity, without advancing any watermark (see
    ``tests/test_timescale_retention_interplay.py``'s pinned boundary).
    A row older than the floor but living in a young chunk is dropped by
    the policy when the chunk itself ages past the boundary, at most one
    chunk interval after the row crosses the floor — chunk granularity,
    not row precision, is the trade.
    """
    # The row-parse lives INSIDE the same fail-open try as the fetch:
    # the fetch and the parse are one probe, and any probe failure —
    # including the parse hitting a row shape it cannot read (no
    # ``drop_after``/``db_now`` key) — must answer None, never raise a
    # naked KeyError through the sweep.  The except below is the whole
    # contract: probe/parse failure → debug log → None → the caller's
    # sweep runs full-range, byte-identical to vanilla behavior.
    try:
        rows = await conn.fetch(_RETENTION_POLICY_FLOOR_PROBE_SQL, schema, table, partition_col)
        if not rows or rows[0]["drop_after"] is None:
            return None
        drop_after: timedelta = rows[0]["drop_after"]
        floor_now: datetime = now if now is not None else rows[0]["db_now"]
        return floor_now - drop_after
    except Exception as exc:  # Why: ANY probe or parse failure must fail open to None — on vanilla Postgres this is the UndefinedTable the views' absence raises, no probe error may ever break a sweep, and a fetched row without the probe's own keys (a stub conn) reads as "no answer", not a KeyError.
        logger.debug(
            "retention_policy_floor_probe_failed schema=%s table=%s error=%r",
            schema,
            table,
            exc,
        )
        return None

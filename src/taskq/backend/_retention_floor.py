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

The probe needs none of the conversion machinery: it is one catalog
query against a connection the CALLER owns and hands in, so it lives
here with no module-level asyncpg import — the connection parameter is
typed through ``TYPE_CHECKING`` plus the future-annotations string
forms, and the runtime imports are stdlib-only.  Deploy-step code that
wants the probe imports it from here; :mod:`taskq.timescale` imports
only this module's cache-bust (the enable path's install-confirmation
duty), never the floor itself — the sweeps call the floor, and the
import chain ``taskq.testing → backend/_sweeps → this module`` stays
stdlib-only (the no-transitive-heavy-deps contract,
``tests/test_memory_jobs_fixture.py::test_testing_no_transitive_asyncpg``).

The floor itself: is *schema.table* a hypertable whose aged end is
owned by a registered, SCHEDULED TimescaleDB retention policy, and where
does that ownership begin — ``now - drop_after``, parsed from the
policy's own registered config, never re-derived from TaskQ's settings.
(A paused job — ``alter_job(job_id, scheduled => FALSE)`` — is
registered but never runs, so it owns nothing: see the None cases on
:func:`retention_policy_floor`.)
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta
from typing import TYPE_CHECKING
from weakref import WeakKeyDictionary, ref

if TYPE_CHECKING:
    from taskq.backend._protocol import ConnLike

__all__ = ["clear_timescaledb_availability_cache", "retention_policy_floor"]

logger = logging.getLogger(__name__)

# The availability gate: the pre-check's statement (one unprivileged
# catalog read — is the extension installed on this server?) and the
# answer's cache. The cache keys the caller's underlying connection,
# weakly — the pool hands the sweeps a PoolConnectionProxy that is
# __slots__-only (un-weak-referenceable) and minted fresh per acquire,
# so the wrapped connection is the stable identity; a multi-server
# process keys per server naturally. A caller that can be neither
# unwrapped nor weak-referenced nor hashed is answered without
# memoizing; every cache interaction is best-effort — the cache can
# never raise through the sweep. Cached answers expire after
# _TIMESCALEDB_ANSWER_TTL_SECONDS (the deploy conversion runs in the
# migrate process while the floor runs in the workers', so no other
# bound retires a worker's cached answer); the enable path's
# clear_timescaledb_availability_cache() is the instant face of the
# same bound.
_TIMESCALEDB_PRESENT_PROBE_SQL = "SELECT 1 FROM pg_extension WHERE extname = 'timescaledb'"

_TIMESCALEDB_ANSWER_TTL_SECONDS = 300.0

# The answer cache: key = the caller's underlying connection (weakly
# held), value = (the answer, the answer's monotonic stamp).
_TIMESCALEDB_PRESENT: WeakKeyDictionary[ConnLike, tuple[bool, float]] = WeakKeyDictionary()

# Busts bump this so a probe already in flight cannot land its stale
# answer behind the bust.
_cache_generation = 0

# The TTL's clock, on the module seam for the tests' injected clock.
_MONOTONIC = time.monotonic


def _availability_cache_key(conn: ConnLike) -> ConnLike | None:
    """The cache key: the proxy's underlying connection when the caller
    hands a pool proxy, the connection itself otherwise — None when the
    candidate cannot be weakly referenced or hashed (the answer is then
    correct but not memoized). Guarded: the cache may never raise
    through the sweep."""
    try:
        underlying = getattr(conn, "_con", None)
    except Exception:  # Why: a wrapper whose attribute access itself raises is an unkeyable caller, not a sweep failure — answered uncached below.
        return None
    candidate = conn if underlying is None else underlying
    try:
        ref(candidate)
        hash(candidate)
    except Exception:  # Why: weak-referenceability and hashability are both cache mechanics, not probe semantics — an unkeyable caller is answered uncached, never a TypeError through the sweep.
        return None
    return candidate


def clear_timescaledb_availability_cache() -> None:
    """The deploy-step conversion's cache-bust, called the moment the
    enable path confirms the extension installed: every connection that
    cached a negative before the install re-asks on its next sweep
    instead of trusting the vanilla answer past the install. The bust's
    generation advances with it, so a probe already in flight cannot
    re-strand a stale negative behind it. (The disable direction needs
    no bust: the extension stays installed — nothing here drops it —
    so the views keep answering, the disarmed policies read as no
    owner, and the answers stay right; the TTL retires the entry in
    time.)"""
    global _cache_generation
    _cache_generation += 1
    _TIMESCALEDB_PRESENT.clear()


# One round trip answering both probe questions: is *schema.table* a
# hypertable with a registered retention policy (the information views'
# join), and if so what is the policy's own horizon.  The policy's
# ``drop_after`` is read out of the registered job's ``config`` — the
# exact interval the policy drops on, not a re-derivation from TaskQ's
# settings, so a policy registered by any release with any interval is
# honored as registered.  ``timescaledb_information.dimensions`` pins the
# hypertable's time column to the caller's *partition_col*: the floor's
# clock is the partition column's, and a caller passing a column the
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

    Answers one catalog question per sweep run (never per batch): is
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
    identical to vanilla behavior — when any of:

    * the server has no ``timescaledb`` extension installed (the
      availability pre-check answers before any catalog statement is
      built; the negative is cached per connection with a TTL, so a
      vanilla deployment pays one ``pg_extension`` read per TTL per
      connection, never one guaranteed-failing probe per sweep),
    * the availability pre-check itself errors (an ambiguous answer —
      returned as None and not cached; the next sweep re-asks),
    * the connection's cached answer predates the extension's install
      under it and the TTL has not expired yet — the enable path busts
      the cache when it confirms the install (instant), and the TTL
      bounds the same staleness from any other path (cross-process
      installs included); those sweeps run full-range in the meantime
      (the vanilla behavior: the sweep's own retention bound still
      governs every deletion),
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
    * the probe or the ROW PARSE fails for any reason — permission
      gaps, extension upgrades, config shapes the cast cannot parse,
      and connection wrappers whose fetched rows are not keyed the way
      the real driver's are (a row without the ``drop_after``/``db_now``
      keys must read as "no answer", never as a crash).

    The probe's template keeps the smoke guard's one plan carve-out:
    ``tests/test_sql_templates_smoke_pg.py`` keeps its parse teeth on
    this probe but excuses it from the PLAN demand a server without the
    views cannot meet (``_PLAN_ON_VANILLA_BY_DESIGN``, keyed on the
    statement's own ``timescaledb_information.hypertables`` marker). At
    RUNTIME the availability gate answers the absent-extension server
    before this statement is built — the statement the guard excuses
    never reaches a vanilla wire. The fail-open below keeps its teeth
    for the residual shapes: the views' permission gaps, an extension
    removed under a long-lived connection, and every parse failure.

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
    # ask whether the extension is installed before building the catalog
    # statement its absence would fail (on vanilla Postgres that statement can
    # never succeed — one guaranteed-failing round trip and one
    # UndefinedTableError exception event per sweep). The answer is cached per
    # underlying connection and expires; the fail-open contract below is
    # unchanged, and the Timescale-present path reaches the same probe bytes as
    # before.
    cache_key = _availability_cache_key(conn)
    cached: tuple[bool, float] | None = None
    if cache_key is not None:
        try:
            cached = _TIMESCALEDB_PRESENT.get(cache_key)
        except Exception:  # Why: the cache lookup is best-effort — a mapping failure is an unmemorized caller, never a sweep failure.
            cached = None
    present: bool | None = None
    if cached is not None and _MONOTONIC() - cached[1] <= _TIMESCALEDB_ANSWER_TTL_SECONDS:
        present = cached[0]
    if present is None:
        generation_at_miss = _cache_generation
        observed_at = _MONOTONIC()
        try:
            available = await conn.fetch(_TIMESCALEDB_PRESENT_PROBE_SQL)
        except Exception as exc:  # Why: the pre-check is part of the fail-open surface — an erroring gate is ambiguity, never an answer; it is not cached, and the next sweep re-asks.
            logger.debug(
                "retention_policy_floor_availability_ambiguous schema=%s table=%s error=%r",
                schema,
                table,
                exc,
            )
            return None
        present = bool(available)
        cached_answer = False
        if cache_key is not None and generation_at_miss == _cache_generation:
            # A write racing the entry's death (or any mapping refusal)
            # degrades to re-asking — the cache is best-effort, the
            # sweep never sees the failure. The stamp is the observation
            # instant (before the fetch), so the TTL bounds the
            # answer's age, not the write's.
            try:
                _TIMESCALEDB_PRESENT[cache_key] = (present, observed_at)
                cached_answer = True
            except Exception:
                cached_answer = False
        if not present:
            logger.debug(
                "retention_policy_floor: timescaledb not present on this server "
                "(answer %s) schema=%s table=%s",
                (
                    "held for the TTL"
                    if cached_answer
                    else "not cached: the bust raced the probe, or the mapping refused"
                )
                if cache_key is not None
                else "not cached: the caller is unkeyable",
                schema,
                table,
            )
    if not present:
        return None

    # The row-parse lives inside the same fail-open try as the fetch:
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
    except Exception as exc:  # Why: any probe or parse failure must fail open to None — no probe error may ever break a sweep (the absent-extension server is answered by the gate above; this except covers the residual shapes: the views' permission gaps, an extension removed under a long-lived connection, and a fetched row without the probe's own keys reading as "no answer", not a KeyError).
        logger.debug(
            "retention_policy_floor_probe_failed schema=%s table=%s error=%r",
            schema,
            table,
            exc,
        )
        return None

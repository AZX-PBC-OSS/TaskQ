"""Operator surface for reading and tuning stored `{schema}.actor_config` rows.

Complements :func:`taskq.worker.startup.sync_actor_config`: that function
only ever *seeds* a row's capacity fields (``max_concurrent``,
``max_pending``, ``result_ttl``) on first registration and otherwise
leaves them untouched. This module is how an operator changes them
afterwards — on a live deployment, without a code change or a worker
restart. All three are re-read by the engine without a restart:

* ``max_concurrent`` — the dispatch query joins ``actor_config`` fresh
  on every dispatch cycle (``taskq/backend/_dispatch_sql.py``); a change
  is effective immediately.
* ``result_ttl`` — the terminal-write UPDATE recomputes
  ``result_expires_at`` from the stored value for every completing job
  (``taskq/backend/_sql_templates.py::mark_succeeded``); a change is
  effective for jobs completing after the write.
* ``max_pending`` — enqueue-side processes hold a TTL-bounded cache of
  this table (``taskq/client/_capacity.py``, default 5s staleness); a
  change is effective fleet-wide within seconds, with no redeploy.

Clearing semantics differ by field on purpose. ``--clear-max-concurrent``
writes NULL, which the dispatch SQL reads as *unlimited* — the SQL
cannot see the code literal once the row exists. ``--clear-max-pending``
and ``--clear-result-ttl`` write NULL, which their enforcement paths
read as *fall back to the ``@actor(...)`` literal* — clearing reverts an
override to the code default.
"""

import math
from dataclasses import dataclass
from typing import Final

import asyncpg

from taskq._json import loads
from taskq.backend._protocol import ConnLike
from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: reusing the canonical identifier regex rather than redefining it
)

__all__ = [
    "UNSET",
    "ActorConfigRow",
    "Unset",
    "get_actor_config",
    "list_actor_configs",
    "set_actor_config_capacity",
]


class Unset:
    """Sentinel distinguishing 'leave unchanged' from an explicit ``None`` (clear)."""

    def __repr__(self) -> str:
        return "UNSET"


UNSET: Final = Unset()


@dataclass(frozen=True, slots=True)
class ActorConfigRow:
    """Snapshot of one `{schema}.actor_config` row."""

    actor: str
    max_concurrent: int | None
    max_pending: int | None
    queue: str
    result_ttl: float | None
    metadata: dict[str, object]
    updated_at: str


_LIST_ACTOR_CONFIG_SQL = """
SELECT actor, max_concurrent, max_pending, queue, result_ttl,
       metadata::text AS metadata, updated_at::text AS updated_at
  FROM "{schema}".actor_config
 ORDER BY actor
""".strip()

_GET_ACTOR_CONFIG_SQL = """
SELECT actor, max_concurrent, max_pending, queue, result_ttl,
       metadata::text AS metadata, updated_at::text AS updated_at
  FROM "{schema}".actor_config
 WHERE actor = $1
""".strip()

# Each capacity column is only overwritten when its paired boolean
# "touch" flag is true; otherwise the CASE expression preserves the
# current value. This lets one statement express "set to N", "clear to
# NULL", and "leave alone" for all three fields without dynamic SQL.
_SET_ACTOR_CONFIG_CAPACITY_SQL = """
UPDATE "{schema}".actor_config
   SET max_concurrent = CASE WHEN $2 THEN $3 ELSE max_concurrent END,
       max_pending    = CASE WHEN $4 THEN $5 ELSE max_pending END,
       result_ttl     = CASE WHEN $6 THEN $7 ELSE result_ttl END,
       updated_at     = clock_timestamp()
 WHERE actor = $1
RETURNING actor, max_concurrent, max_pending, queue, result_ttl,
          metadata::text AS metadata, updated_at::text AS updated_at
""".strip()


def _row_to_dataclass(row: asyncpg.Record) -> ActorConfigRow:
    return ActorConfigRow(
        actor=row["actor"],
        max_concurrent=row["max_concurrent"],
        max_pending=row["max_pending"],
        queue=row["queue"],
        result_ttl=row["result_ttl"],
        metadata=loads(row["metadata"]),
        updated_at=row["updated_at"],
    )


async def list_actor_configs(conn: ConnLike, *, schema: str = "taskq") -> list[ActorConfigRow]:
    """Return every stored `{schema}.actor_config` row, ordered by actor name."""
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")
    rows = await conn.fetch(_LIST_ACTOR_CONFIG_SQL.format(schema=schema))
    return [_row_to_dataclass(row) for row in rows]


async def get_actor_config(
    conn: ConnLike, actor: str, *, schema: str = "taskq"
) -> ActorConfigRow | None:
    """Return the stored row for *actor*, or ``None`` if it has never been synced."""
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")
    row = await conn.fetchrow(_GET_ACTOR_CONFIG_SQL.format(schema=schema), actor)
    return _row_to_dataclass(row) if row is not None else None


def _validate_int_field(name: str, value: int | None | Unset) -> None:
    """Reject the two shapes that slip past ``isinstance(x, int) and x < 0``:
    ``bool`` (an ``int`` subclass — ``False`` would be written as 0, flooring
    the dispatch residual ``GREATEST(cap - in_flight, 0)`` and silently
    pausing the actor) and negative values."""
    if isinstance(value, bool):
        raise ValueError(f"{name} must be a non-negative integer; got {value!r} (bool)")
    if isinstance(value, int) and value < 0:
        raise ValueError(f"{name} must be a non-negative integer; got {value!r}")


def _validate_result_ttl(value: float | None | Unset) -> None:
    """Reject bool, negative, and non-finite ``result_ttl``.

    NaN sails through ``value < 0`` (NaN compares False) and then breaks
    every completion for the actor — ``now() + NaN * interval '1 second'``
    raises ``interval out of range`` in the terminal-write UPDATE. ±inf is
    rejected on the same grounds (``interval out of range`` / meaningless
    expiry)."""
    if isinstance(value, bool):
        raise ValueError(
            f"result_ttl must be a non-negative number of seconds; got {value!r} (bool)"
        )
    if isinstance(value, (int, float)):
        if not math.isfinite(value):
            raise ValueError(f"result_ttl must be finite; got {value!r}")
        if value < 0:
            raise ValueError(f"result_ttl must be a non-negative number of seconds; got {value!r}")


async def set_actor_config_capacity(
    conn: ConnLike,
    actor: str,
    *,
    max_concurrent: int | None | Unset = UNSET,
    max_pending: int | None | Unset = UNSET,
    result_ttl: float | None | Unset = UNSET,
    schema: str = "taskq",
) -> ActorConfigRow | None:
    """Update capacity fields on an existing `{schema}.actor_config` row.

    Only fields passed as something other than :data:`UNSET` are
    changed. Pass ``None`` explicitly to clear a field — precisely what
    that means depends on the field's enforcement path: clearing
    ``max_concurrent`` makes the actor *unlimited* (the dispatch SQL
    reads a stored NULL as no cap), while clearing ``max_pending`` or
    ``result_ttl`` reverts enforcement to the ``@actor(...)`` literal
    (those paths can still see the code default). Returns ``None`` if
    *actor* has no stored row — a row is only created by
    :func:`taskq.worker.startup.sync_actor_config` at worker startup, so
    an actor must have been registered by at least one worker before its
    capacity can be tuned here.

    Raises :class:`ValueError` if ``max_concurrent`` or ``max_pending``
    is a negative integer or a ``bool`` — the same guard ``@actor(...)``
    applies at decoration time (``taskq/actor.py``), plus the bool case
    (``False`` is an ``int`` and would be written as 0). Without these,
    an operator typo here (e.g. ``--max-concurrent -5``) would write
    silently into the dispatch CTE's ``GREATEST(ac.max_concurrent -
    in_flight, 0)`` residual calculation, floor to zero, and pause the
    actor indefinitely with no error anywhere in the path. ``result_ttl``
    is likewise rejected when negative, non-finite, or ``bool`` — a
    negative TTL would set ``result_expires_at`` to a past timestamp in
    the terminal-write UPDATE (silently expiring every result the moment
    it is written), and NaN/±inf raise ``interval out of range`` in that
    same UPDATE, failing every completion for the actor.
    """
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")
    _validate_int_field("max_concurrent", max_concurrent)
    _validate_int_field("max_pending", max_pending)
    _validate_result_ttl(result_ttl)

    row = await conn.fetchrow(
        _SET_ACTOR_CONFIG_CAPACITY_SQL.format(schema=schema),
        actor,
        not isinstance(max_concurrent, Unset),
        None if isinstance(max_concurrent, Unset) else max_concurrent,
        not isinstance(max_pending, Unset),
        None if isinstance(max_pending, Unset) else max_pending,
        not isinstance(result_ttl, Unset),
        None if isinstance(result_ttl, Unset) else result_ttl,
    )
    return _row_to_dataclass(row) if row is not None else None

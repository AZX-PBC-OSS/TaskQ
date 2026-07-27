"""Operator surface for reading and tuning stored `{schema}.actor_config` rows.

Complements :func:`taskq.worker.startup.sync_actor_config`: that function
only ever *seeds* a row's capacity fields (``max_concurrent``,
``max_pending``, ``result_ttl``) on first registration and otherwise
leaves them untouched. This module is how an operator changes them
afterwards — on a live deployment, without a code change or a worker
restart for the fields the dispatch/consume paths re-read per operation
(``max_concurrent`` at every dispatch cycle, ``result_ttl`` at every
terminal write; see ``taskq/backend/_dispatch_sql.py`` and
``taskq/backend/_sql_templates.py::mark_succeeded`` respectively).

``max_pending`` has **no live effect**: `enqueue()`'s pre-flight limit
check compares against the in-process ``@actor(max_pending=...)``
literal (``taskq/client/_args.py``), never against this table. Because
of that, ``taskq actor-config set`` deliberately does NOT expose a
``--max-pending`` flag — a flag whose write is never read by the engine
is exactly the "documented capability the engine doesn't deliver"
defect this module exists to avoid. :func:`set_actor_config_capacity`
still accepts a ``max_pending`` keyword for callers with a legitimate
reason to touch the stored value directly (observability tooling,
re-seeding a dropped row) — that is a conscious asymmetry between the
function and the CLI, not an oversight.
"""

from dataclasses import dataclass
from typing import Final

import asyncpg

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
    updated_at: str


_LIST_ACTOR_CONFIG_SQL = """
SELECT actor, max_concurrent, max_pending, queue, result_ttl, updated_at::text AS updated_at
  FROM "{schema}".actor_config
 ORDER BY actor
""".strip()

_GET_ACTOR_CONFIG_SQL = """
SELECT actor, max_concurrent, max_pending, queue, result_ttl, updated_at::text AS updated_at
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
       updated_at     = now()
 WHERE actor = $1
RETURNING actor, max_concurrent, max_pending, queue, result_ttl, updated_at::text AS updated_at
""".strip()


def _row_to_dataclass(row: asyncpg.Record) -> ActorConfigRow:
    return ActorConfigRow(
        actor=row["actor"],
        max_concurrent=row["max_concurrent"],
        max_pending=row["max_pending"],
        queue=row["queue"],
        result_ttl=row["result_ttl"],
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
    changed; pass ``None`` explicitly to clear a field back to
    unlimited/default. Returns ``None`` if *actor* has no stored row —
    a row is only created by :func:`taskq.worker.startup.sync_actor_config`
    at worker startup, so an actor must have been registered by at least
    one worker before its capacity can be tuned here.

    Raises :class:`ValueError` if ``max_concurrent`` or ``max_pending``
    is a negative integer — the same guard ``@actor(...)`` applies at
    decoration time (``taskq/actor.py``). Without it, an operator typo
    here (e.g. ``--max-concurrent -5``) would write silently into the
    dispatch CTE's ``GREATEST(ac.max_concurrent - in_flight, 0)``
    residual calculation, floor to zero, and pause the actor
    indefinitely with no error anywhere in the path.  ``result_ttl``
    is likewise rejected when negative — a negative TTL would set
    ``result_expires_at`` to a past timestamp in the terminal-write
    UPDATE, silently expiring every result the moment it is written.
    """
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")
    if isinstance(max_concurrent, int) and max_concurrent < 0:
        raise ValueError(f"max_concurrent must be a non-negative integer; got {max_concurrent!r}")
    if isinstance(max_pending, int) and max_pending < 0:
        raise ValueError(f"max_pending must be a non-negative integer; got {max_pending!r}")
    if isinstance(result_ttl, (int, float)) and result_ttl < 0:
        raise ValueError(f"result_ttl must be a non-negative number of seconds; got {result_ttl!r}")

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

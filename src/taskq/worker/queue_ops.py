"""Operator surface for reading and configuring `{schema}.queues` rows.

The `queues` table drives two features that are otherwise unreachable:

* ``mode`` selects the dispatch ordering. It defaults to ``strict_fifo``,
  and round-robin fairness engages only when it is ``round_robin`` -- so
  a job's ``fairness_key`` is carried through the dispatch CTE and then
  discarded unless a row here says otherwise.
* ``max_concurrent`` is the per-queue leased-slot concurrency cap, read
  once at worker startup.

Nothing else in TaskQ writes this table: no migration seeds it and no
bootstrap step creates rows. Before this module the only documented
remedy was raw SQL, and the form the docs gave --
``UPDATE "taskq".queues SET mode = 'round_robin' WHERE name = 'x'`` --
silently affects **zero rows** when the row does not exist, which it
never does by default. That is why configuring fairness appeared to
succeed and then did nothing.

:func:`set_queue_mode` and :func:`set_queue_max_concurrent` therefore
UPSERT rather than UPDATE, so configuring a queue works on a fresh
deployment.
"""

from dataclasses import dataclass
from typing import Final

from taskq.backend._protocol import ConnLike
from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: reusing the canonical identifier regex rather than redefining it
)

__all__ = [
    "QUEUE_MODES",
    "QueueRow",
    "get_queue",
    "list_queues",
    "set_queue_max_concurrent",
    "set_queue_mode",
]

#: The values the table's CHECK constraint admits.
QUEUE_MODES: Final = ("strict_fifo", "round_robin")

#: The mode a queue has when it has no row at all. Must match the column
#: DEFAULT in 01.00.00_01_pre_initial.sql and the fallback in
#: taskq.backend._dispatch._resolve_queue_modes.
DEFAULT_QUEUE_MODE: Final = "strict_fifo"


@dataclass(frozen=True, slots=True)
class QueueRow:
    """One stored `{schema}.queues` row."""

    name: str
    mode: str
    max_concurrent: int | None


def _check_schema(schema: str) -> None:
    if not _IDENT_RE.match(schema):
        msg = f"invalid schema name: {schema!r}"
        raise ValueError(msg)


async def list_queues(conn: ConnLike, *, schema: str = "taskq") -> list[QueueRow]:
    """Every stored queue row, ordered by name."""
    _check_schema(schema)
    rows = await conn.fetch(
        f'SELECT name, mode, max_concurrent FROM "{schema}".queues ORDER BY name'  # noqa: S608  # Why: schema validated against _IDENT_RE above and double-quoted; no user values interpolated.
    )
    return [
        QueueRow(name=r["name"], mode=r["mode"], max_concurrent=r["max_concurrent"]) for r in rows
    ]


async def get_queue(conn: ConnLike, name: str, *, schema: str = "taskq") -> QueueRow | None:
    """One stored queue row, or ``None`` when the queue has never been configured.

    ``None`` is not "the queue does not exist" -- queues are implicit, created
    by enqueueing onto them. It means the queue runs on the defaults
    (``strict_fifo``, no concurrency cap).
    """
    _check_schema(schema)
    row = await conn.fetchrow(
        f'SELECT name, mode, max_concurrent FROM "{schema}".queues WHERE name = $1',  # noqa: S608  # Why: schema validated against _IDENT_RE above and double-quoted; name is $1-bound.
        name,
    )
    if row is None:
        return None
    return QueueRow(name=row["name"], mode=row["mode"], max_concurrent=row["max_concurrent"])


async def set_queue_mode(
    conn: ConnLike, name: str, mode: str, *, schema: str = "taskq"
) -> QueueRow:
    """Set a queue's dispatch ordering mode, creating the row if absent.

    UPSERT, not UPDATE: a plain UPDATE is a silent no-op on a fresh
    deployment, because nothing in TaskQ ever inserts a `queues` row.
    """
    _check_schema(schema)
    if mode not in QUEUE_MODES:
        msg = f"invalid queue mode {mode!r}; must be one of {', '.join(QUEUE_MODES)}"
        raise ValueError(msg)
    row = await conn.fetchrow(
        f'INSERT INTO "{schema}".queues (name, mode) VALUES ($1, $2) '  # noqa: S608  # Why: schema validated against _IDENT_RE above and double-quoted; values are $n-bound.
        "ON CONFLICT (name) DO UPDATE SET mode = EXCLUDED.mode, updated_at = clock_timestamp() "
        "RETURNING name, mode, max_concurrent",
        name,
        mode,
    )
    assert row is not None  # RETURNING on an upsert always yields a row
    return QueueRow(name=row["name"], mode=row["mode"], max_concurrent=row["max_concurrent"])


async def set_queue_max_concurrent(
    conn: ConnLike, name: str, max_concurrent: int | None, *, schema: str = "taskq"
) -> QueueRow:
    """Set (or clear, with ``None``) a queue's leased-slot concurrency cap.

    Unlike ``actor_config.max_concurrent``, this is read once at worker
    startup, so a change needs a worker restart to take effect.

    ``max_concurrent`` must be ``>= 1`` or ``None`` — the table's CHECK
    constraint (``max_concurrent IS NULL OR max_concurrent >= 1``)
    enforces the same, but rejecting here saves the operator from a raw
    asyncpg ``CheckViolationError`` traceback. ``None`` is the uncapped
    state; a 0 "emergency drain" belongs to
    ``actor_config.max_concurrent`` (per-actor), which legitimately
    allows it.
    """
    _check_schema(schema)
    if max_concurrent is not None and max_concurrent < 1:
        msg = (
            f"max_concurrent must be >= 1 or None (NULL = uncapped), got {max_concurrent!r}; "
            "an emergency drain to 0 belongs to actor_config.max_concurrent"
        )
        raise ValueError(msg)
    row = await conn.fetchrow(
        f'INSERT INTO "{schema}".queues (name, max_concurrent) VALUES ($1, $2) '  # noqa: S608  # Why: schema validated against _IDENT_RE above and double-quoted; values are $n-bound.
        "ON CONFLICT (name) DO UPDATE SET max_concurrent = EXCLUDED.max_concurrent, "
        "updated_at = clock_timestamp() RETURNING name, mode, max_concurrent",
        name,
        max_concurrent,
    )
    assert row is not None
    return QueueRow(name=row["name"], mode=row["mode"], max_concurrent=row["max_concurrent"])

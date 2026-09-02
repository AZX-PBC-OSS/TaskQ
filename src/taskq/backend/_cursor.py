"""Cross-backend cursor encoding and orderings for keyset pagination.

Both :class:`PostgresBackend` and :class:`InMemoryBackend` must agree on
cursor encoding and comparison semantics (``JobFilter.cursor`` docstring).
This module is the canonical location for that contract — it lives in
``taskq.backend`` so that production code can import it without depending
on the ``taskq.testing`` package.

An ordering and the cursor that pages it are **one object**, not two
parallel switch statements: :class:`JobOrdering` owns the ORDER BY
columns, the cursor text those columns encode to, the SQL keyset
predicate and the in-memory comparison, all derived from the same
:class:`SortColumn` tuple.  A sort direction can therefore never drift
away from the cursor that is supposed to seam it — the failure mode that
made every non-default ``order_by`` reject cursors outright.
"""

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Final, Literal
from uuid import UUID

from taskq.backend._protocol import JobRow, JobSortField

__all__ = [
    "ORDERINGS",
    "CursorValue",
    "JobOrdering",
    "SortColumn",
    "decode_batch_cursor",
    "decode_cursor",
    "decode_job_cursor",
    "encode_batch_cursor",
    "encode_cursor",
    "encode_job_cursor",
    "ordering_for",
]


def encode_cursor(priority: int, scheduled_at: datetime, job_id: UUID) -> str:
    """Encode keyset pagination cursor as ``priority|iso|uuid``.

    The loose-argument form of the default ordering's cursor, kept for
    callers that hold the three values rather than a :class:`JobRow`.
    ``ORDERINGS[JobSortField.SCHEDULED_AT_ASC].encode`` renders the same
    text from a row; ``test_cursor_orderings`` pins the two together.
    """
    return f"{priority}|{scheduled_at.isoformat()}|{job_id}"


def decode_cursor(cursor: str) -> tuple[int, datetime, UUID]:
    """Decode the default ordering's keyset cursor."""
    parts = cursor.split("|", 2)
    if len(parts) != 3:
        raise ValueError(f"Invalid cursor format: {cursor!r}")
    priority = int(parts[0])
    scheduled_at = datetime.fromisoformat(parts[1])
    job_id = UUID(parts[2])
    return priority, scheduled_at, job_id


def encode_batch_cursor(created_at: datetime, batch_id: UUID) -> str:
    """Encode a batch keyset cursor as ``iso|uuid``.

    Same ``|``-delimited shape as :func:`encode_cursor`, one field
    shorter: ``list_batches`` orders on ``(created_at, id)`` where
    ``list_jobs``' default ordering orders on ``(priority, scheduled_at,
    id)``.
    """
    return f"{created_at.isoformat()}|{batch_id}"


def decode_batch_cursor(cursor: str) -> tuple[datetime, UUID]:
    """Decode a batch keyset cursor to the columns' own Python types.

    Returning a ``datetime`` and a ``UUID`` -- not the raw text -- is
    load-bearing, not tidiness: asyncpg infers each placeholder's type
    from its ``::`` cast and refuses a ``str`` for ``timestamptz`` or
    ``uuid``, which is what made every admin job-list page turn raise
    ``DataError`` before 2569da5.
    """
    parts = cursor.split("|", 1)
    if len(parts) != 2:
        raise ValueError(f"Invalid batch cursor format: {cursor!r}")
    return datetime.fromisoformat(parts[0]), UUID(parts[1])


# ── orderings ───────────────────────────────────────────────────────────

type ColumnKind = Literal["int", "text", "ts", "uuid"]
type CursorValue = int | str | datetime | UUID | None

#: asyncpg types every placeholder from its ``::`` cast, so the cast and
#: the Python type the cursor field parses to are the same decision.
_CASTS: Final[Mapping[ColumnKind, str]] = {
    "int": "::int",
    "text": "",
    "ts": "::timestamptz",
    "uuid": "::uuid",
}


@dataclass(frozen=True, slots=True)
class SortColumn:
    """One column of an ordering, together with how the cursor carries it.

    ``nullable`` means the column participates in NULLS LAST placement:
    NULLs sort after every value in **both** directions, so that reversing
    the scan reverses one thing (the comparison) rather than two.
    """

    name: str
    kind: ColumnKind
    descending: bool
    nullable: bool = False

    @property
    def cast(self) -> str:
        return _CASTS[self.kind]

    def format(self, value: CursorValue) -> str:
        """Render *value* as a cursor field; ``None`` becomes empty."""
        if value is None:
            return ""
        return value.isoformat() if isinstance(value, datetime) else str(value)

    def parse(self, text: str) -> CursorValue:
        """Parse a cursor field back to the column's own Python type.

        Never returns the raw text for a typed column: binding a ``str``
        to a ``timestamptz``/``uuid``/``int`` placeholder is the asyncpg
        ``DataError`` that 500'd every admin page turn before 2569da5.
        """
        if text == "":
            if not self.nullable:
                raise ValueError(f"empty cursor field for non-nullable column {self.name!r}")
            return None
        match self.kind:
            case "int":
                return int(text)
            case "ts":
                return datetime.fromisoformat(text)
            case "uuid":
                return UUID(text)
            case "text":
                return text


#: ``id`` is UUIDv7 and therefore time-ordered, so ordering it *with* the
#: primary column reorders nothing in practice while making the seam a
#: single row-wise comparison.  Ordering it against the primary column is
#: what made a keyset cursor inexpressible for the DESC sorts.
_ID_ASC: Final = SortColumn("id", "uuid", descending=False)
_ID_DESC: Final = SortColumn("id", "uuid", descending=True)


def _lt(x: CursorValue, y: CursorValue) -> bool:
    """Order two cursor values of the same column (never NULL).

    Dispatching on the runtime type rather than declaring a comparable
    protocol keeps the union honest: the four kinds are mutually
    incomparable and a mismatch is a bug, not a silent ordering.
    """
    if isinstance(x, datetime) and isinstance(y, datetime):
        return x < y
    if isinstance(x, UUID) and isinstance(y, UUID):
        return x < y
    if isinstance(x, bool) or isinstance(y, bool):
        raise TypeError("bool is not a sort column kind")
    if isinstance(x, int) and isinstance(y, int):
        return x < y
    if isinstance(x, str) and isinstance(y, str):
        return x < y
    raise TypeError(f"incomparable cursor values: {x!r}, {y!r}")


@dataclass(frozen=True, slots=True)
class JobOrdering:
    """An ORDER BY and the cursor that pages it, as one object.

    Every consumer derives from :attr:`columns`: the SQL ORDER BY, the
    SQL keyset predicate, the cursor text and the in-memory comparison.
    Adding an ordering is one tuple, and a direction cannot be changed in
    the sort without changing it in the cursor.
    """

    columns: tuple[SortColumn, ...]

    # ── SQL rendering ───────────────────────────────────────────────
    def order_by_sql(self, *, forward: bool = True) -> str:
        """Render the ORDER BY column list.

        ``forward=False`` renders the reverse scan used by a "previous
        page" query, which flips NULLS placement as well as the
        directions -- reversing only the directions leaves the NULL
        range stranded at the wrong end.
        """
        terms: list[str] = []
        for col in self.columns:
            descending = col.descending == forward
            term = f"{col.name} {'DESC' if descending else 'ASC'}"
            if col.nullable:
                term += " NULLS LAST" if forward else " NULLS FIRST"
            terms.append(term)
        return ", ".join(terms)

    def sql_after(
        self,
        values: tuple[CursorValue, ...],
        first_param: int,
        *,
        forward: bool = True,
    ) -> tuple[str, list[object]]:
        """Render the keyset predicate for rows strictly past *values*.

        Returns the SQL and the parameters to bind at ``$first_param``
        onwards.  A ``None`` in *values* binds no parameter — it is a
        NULL seam, expressed as ``IS NULL`` rather than a comparison,
        because ``col < NULL`` is NULL and would drop the whole range.
        """
        slots: list[int | None] = []
        params: list[object] = []
        for value in values:
            if value is None:
                slots.append(None)
            else:
                params.append(value)
                slots.append(first_param + len(params) - 1)

        descending = [col.descending == forward for col in self.columns]
        if len(set(descending)) == 1 and not any(c.nullable for c in self.columns[1:]):
            return self._row_wise_sql(values, slots, forward=forward), params
        return self._lexicographic_sql(values, slots, descending, forward=forward), params

    def _row_wise_sql(
        self, values: tuple[CursorValue, ...], slots: list[int | None], *, forward: bool
    ) -> str:
        """One row-wise tuple comparison — the ``list_batches`` shape.

        Available exactly when every column runs the same way, which is
        what ordering ``id`` with the primary column buys.  A leading
        nullable column splits the scan into its NULL and non-NULL
        ranges: under NULLS LAST the NULL range is wholly after the
        values, so it is a union with the tuple compare rather than a
        term inside it.
        """
        op = "<" if self.columns[0].descending == forward else ">"
        lead = self.columns[0]
        if lead.nullable and values[0] is None:
            # The seam is inside the NULL range: only the columns after
            # the NULL one still discriminate.
            tail = _tuple_cmp(self.columns[1:], slots[1:], op)
            inside = f"({lead.name} IS NULL AND {tail})"
            return inside if forward else f"({inside} OR {lead.name} IS NOT NULL)"
        whole = _tuple_cmp(self.columns, slots, op)
        if lead.nullable and forward:
            return f"({lead.name} IS NULL OR {whole})"
        return whole

    def _lexicographic_sql(
        self,
        values: tuple[CursorValue, ...],
        slots: list[int | None],
        descending: list[bool],
        *,
        forward: bool,
    ) -> str:
        """Expanded seam for a mixed-direction ordering.

        The default ordering runs ``priority DESC, scheduled_at ASC``, so
        no single tuple comparison describes it — a row-wise compare can
        only express a seam when every column sorts the same way.  Same
        descriptor, same directions, one term per column.
        """
        terms: list[str] = []
        equalities: list[str] = []
        for col, value, slot, desc in zip(self.columns, values, slots, descending, strict=True):
            strict = _strict_cmp(col, value, slot, descending=desc, forward=forward)
            if strict is not None:
                terms.append(" AND ".join([*equalities, strict]))
            equalities.append(_equality_cmp(col, slot))
        return "(" + " OR ".join(f"({t})" for t in terms) + ")"

    # ── cursor text ─────────────────────────────────────────────────
    def encode(self, row: JobRow) -> str:
        """Encode *row* as this ordering's cursor."""
        return "|".join(
            col.format(value) for col, value in zip(self.columns, self.values(row), strict=True)
        )

    def decode(self, cursor: str) -> tuple[CursorValue, ...]:
        """Decode this ordering's cursor to the columns' own types."""
        parts = cursor.split("|", len(self.columns) - 1)
        if len(parts) != len(self.columns):
            raise ValueError(f"Invalid cursor format: {cursor!r}")
        return tuple(col.parse(part) for col, part in zip(self.columns, parts, strict=True))

    # ── in-memory comparison ────────────────────────────────────────
    def values(self, row: JobRow) -> tuple[CursorValue, ...]:
        return tuple(_ROW_READERS[col.name](row) for col in self.columns)

    def compare(self, left: tuple[CursorValue, ...], right: tuple[CursorValue, ...]) -> int:
        """Three-way compare two key tuples under this ordering.

        The in-memory mirror of the SQL ORDER BY, including NULLS LAST:
        nullness is decided before direction is applied, because NULLS
        LAST is absolute and does not flip with ``DESC``.
        """
        for col, x, y in zip(self.columns, left, right, strict=True):
            if x is None or y is None:
                if x is None and y is None:
                    continue
                return 1 if x is None else -1
            if _lt(x, y):
                return 1 if col.descending else -1
            if _lt(y, x):
                return -1 if col.descending else 1
        return 0

    def compare_rows(self, left: JobRow, right: JobRow) -> int:
        return self.compare(self.values(left), self.values(right))


def _tuple_cmp(columns: tuple[SortColumn, ...], slots: list[int | None], op: str) -> str:
    """``(a, b) < ($1::ts, $2::uuid)``, or the one-column form."""
    names = ", ".join(col.name for col in columns)
    binds = ", ".join(f"${slot}{col.cast}" for col, slot in zip(columns, slots, strict=True))
    if len(columns) == 1:
        return f"{names} {op} {binds}"
    return f"({names}) {op} ({binds})"


def _equality_cmp(col: SortColumn, slot: int | None) -> str:
    return f"{col.name} IS NULL" if slot is None else f"{col.name} = ${slot}{col.cast}"


def _strict_cmp(
    col: SortColumn, value: CursorValue, slot: int | None, *, descending: bool, forward: bool
) -> str | None:
    """Rows strictly past the cursor on *col* alone, or ``None`` if none can be.

    Under NULLS LAST nothing follows a NULL going forwards, so a NULL
    seam contributes no term at that position — the columns after it
    carry the seam.  Going backwards from a NULL, every non-NULL row
    precedes it.
    """
    op = "<" if descending else ">"
    if col.nullable and value is None:
        return None if forward else f"{col.name} IS NOT NULL"
    comparison = f"{col.name} {op} ${slot}{col.cast}"
    if col.nullable and forward:
        return f"({col.name} IS NULL OR {comparison})"
    return comparison


_ROW_READERS: Final[Mapping[str, Callable[[JobRow], CursorValue]]] = {
    "id": lambda r: r.id,
    "priority": lambda r: r.priority,
    "scheduled_at": lambda r: r.scheduled_at,
    "created_at": lambda r: r.created_at,
    "finished_at": lambda r: r.finished_at,
}


ORDERINGS: Final[Mapping[JobSortField, JobOrdering]] = {
    # Unchanged, and the one mixed-direction ordering: dispatch order is
    # highest priority first, then oldest schedule first.  ``id ASC``
    # already runs with ``scheduled_at ASC``, so its cursor was always
    # expressible and its encoding is preserved byte for byte.
    JobSortField.SCHEDULED_AT_ASC: JobOrdering(
        (
            SortColumn("priority", "int", descending=True),
            SortColumn("scheduled_at", "ts", descending=False),
            _ID_ASC,
        )
    ),
    JobSortField.CREATED_AT_DESC: JobOrdering(
        (SortColumn("created_at", "ts", descending=True), _ID_DESC)
    ),
    JobSortField.FINISHED_AT_DESC: JobOrdering(
        (
            SortColumn("finished_at", "ts", descending=True, nullable=True),
            _ID_DESC,
        )
    ),
}


def ordering_for(order_by: JobSortField | None) -> JobOrdering:
    """Resolve ``JobFilter.order_by`` (``None`` is the default ordering)."""
    return ORDERINGS[order_by if order_by is not None else JobSortField.SCHEDULED_AT_ASC]


def encode_job_cursor(row: JobRow, order_by: JobSortField | None) -> str:
    """Encode *row* as the cursor for *order_by*'s ordering."""
    return ordering_for(order_by).encode(row)


def decode_job_cursor(cursor: str, order_by: JobSortField | None) -> tuple[CursorValue, ...]:
    """Decode a cursor under *order_by*'s ordering, to the columns' types."""
    return ordering_for(order_by).decode(cursor)

"""Shared filter→SQL WHERE condition builder.

Extracted from ``_reads._list_jobs`` so that ``cancel_where`` (bulk
cancel) reuses the exact same filter logic. Only predicate fields
(queue, status, actor, identity_key, batch_id, tags, active) are
translated to conditions. The ``cursor``, ``limit``, and ``order_by``
fields are NOT handled here — callers apply them separately.

This module is SQL-only; the in-memory backend filters via its own
implementation in ``testing/_reads.py``. Backend equivalence is enforced
by the shared test suite (``test_backend_equivalence.py``), not shared
code — the two filter-matching strategies (SQL WHERE vs Python
predicates) do not share a trivial interface.
"""

from dataclasses import dataclass

from taskq._json import dumps_str
from taskq.backend._protocol import JobFilter
from taskq.backend.statemachine import ACTIVE_STATUSES, TERMINAL_STATUSES

__all__ = ["FilterSQL", "build_filter_conditions"]


@dataclass(frozen=True, slots=True)
class FilterSQL:
    """Built SQL fragments and parameters from a JobFilter.

    ``conditions`` and ``params`` are stored as tuples so the frozen
    contract is meaningful (mutable list fields in a frozen dataclass
    only prevent reassignment, not in-place mutation).
    """

    conditions: tuple[str, ...] = ()
    params: tuple[object, ...] = ()


def build_filter_conditions(filter: JobFilter) -> FilterSQL:
    """Build WHERE clause conditions and parameters from a JobFilter.

    Shared between ``_list_jobs`` (reads) and ``cancel_where`` (writes)
    so the filter semantics are identical for query and mutation.

    Only predicate fields (queue, status, actor, identity_key, batch_id,
    tags, active) are translated to conditions. The ``cursor``, ``limit``,
    and ``order_by`` fields are NOT handled here — callers apply them
    separately:

    - ``_list_jobs`` appends the cursor keyset condition and LIMIT/OFFSET
      after calling this helper (preserving the existing behavior).
    - ``cancel_where`` ignores cursor/limit/order_by entirely (bulk writes
      are not paginated).
    """
    conditions: list[str] = []
    params: list[object] = []
    n = 0

    def _next_param(expr: str) -> str:
        nonlocal n
        n += 1
        return f"{expr} = ${n}"

    def _next_any_param(expr: str) -> str:
        nonlocal n
        n += 1
        return f"{expr} = ANY(${n})"

    if filter.queue is not None:
        conditions.append(_next_param("queue"))
        params.append(filter.queue)

    if filter.status is not None:
        if isinstance(filter.status, str):
            conditions.append(_next_param("status"))
            params.append(filter.status)
        else:
            conditions.append(_next_any_param("status"))
            params.append(list(filter.status))
    elif filter.active is not None:
        statuses = list(ACTIVE_STATUSES) if filter.active else list(TERMINAL_STATUSES)
        conditions.append(_next_any_param("status"))
        params.append(statuses)

    if filter.actor is not None:
        conditions.append(_next_param("actor"))
        params.append(filter.actor)

    if filter.identity_key is not None:
        conditions.append(_next_param("identity_key"))
        params.append(filter.identity_key)

    if filter.batch_id is not None:
        n += 1
        conditions.append(f"metadata @> ${n}::jsonb")
        params.append(dumps_str({"batch_id": str(filter.batch_id)}))

    if filter.tags is not None and len(filter.tags) > 0:
        n += 1
        conditions.append(f"tags && ${n}::text[]")
        params.append(list(filter.tags))

    return FilterSQL(conditions=tuple(conditions), params=tuple(params))

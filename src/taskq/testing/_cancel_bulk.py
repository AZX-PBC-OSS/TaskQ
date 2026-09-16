"""Bulk cancel implementation for InMemoryBackend.

Module-level function following the same pattern as ``testing/_reads.py``,
``testing/_terminal.py``, etc.
"""

from dataclasses import replace as dc_replace
from typing import TYPE_CHECKING
from uuid import UUID

from taskq.backend._protocol import BulkCancelResult, CancelPhase, JobFilter
from taskq.constants import CANCEL_ORIGIN_PENDING

if TYPE_CHECKING:
    from taskq.testing.in_memory import InMemoryBackend

__all__ = ["_cancel_where"]


async def _cancel_where(
    self: "InMemoryBackend",
    filter: JobFilter,
    reason: str | None,
) -> BulkCancelResult:
    from taskq.testing._reads import _list_jobs

    # Sanitize the filter: cancel_where ignores limit, cursor, and order_by.
    # Use a very large limit instead of None because JobFilter.limit is typed
    # as int (not int | None) with a __post_init__ guard against negatives.
    # cursor=None disables keyset slicing; order_by=None drops the caller's
    # paging order, which cancel does not owe (JobFilter.order_by docstring).
    sanitized = dc_replace(filter, limit=2**31, cursor=None, order_by=None)
    rows = await _list_jobs(self, sanitized)
    # cancel_where owes job-id-ascending ids: the PG statement returns
    # ``array_agg(id ORDER BY id)`` over driving windows that are themselves
    # ``ORDER BY id`` (backend/_cancel_bulk.py), so each batch's ids — and
    # the drain's concatenation of batches — come back UUID-ascending. No
    # JobSortField is id-ascending (``ordering_for``'s default orders
    # priority first), so the id order is imposed on the listed rows here.
    rows.sort(key=lambda r: r.id)

    cancelled_ids: list[UUID] = []
    cancel_requested_ids: list[UUID] = []

    for row in rows:
        if row.status in ("pending", "scheduled"):
            now = self._clock.now()
            self._jobs[row.id] = dc_replace(
                row,
                status="cancelled",
                finished_at=now,
                # Twin of the PG drain's cancelled CTE: the bulk path stamps
                # the same before-start origin marker the single-job
                # write_cancel_request path stamps, on the row only.
                error_class=CANCEL_ORIGIN_PENDING,
            )
            self._append_state_change_event(
                job_id=row.id,
                from_state=row.status,
                to_state="cancelled",
                now=now,
            )
            self._append_cancel_request_event(row.id, now, reason)
            cancelled_ids.append(row.id)

        elif row.status == "running" and row.cancel_phase == CancelPhase.NONE:
            now = self._clock.now()
            self._jobs[row.id] = dc_replace(
                row,
                cancel_requested_at=now,
                cancel_phase=CancelPhase.COOPERATIVE,
            )
            self._append_cancel_request_event(row.id, now, reason)
            for event in self._cancel_wake_subscribers:
                event.set()
            cancel_requested_ids.append(row.id)

    return BulkCancelResult(
        cancelled_directly=len(cancelled_ids),
        cancel_requested=len(cancel_requested_ids),
        cancelled_ids=tuple(cancelled_ids),
        cancel_requested_ids=tuple(cancel_requested_ids),
    )

"""Bulk cancel implementation for InMemoryBackend.

Module-level function following the same pattern as ``testing/_reads.py``,
``testing/_terminal.py``, etc.
"""

from dataclasses import replace as dc_replace
from typing import TYPE_CHECKING
from uuid import UUID

from taskq.backend._protocol import BulkCancelResult, CancelPhase, JobFilter

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
    # cursor=None disables keyset slicing. order_by=None selects the default
    # priority/scheduled_at/id sort, which is harmless for cancel.
    sanitized = dc_replace(filter, limit=2**31, cursor=None, order_by=None)
    rows = await _list_jobs(self, sanitized)

    cancelled_ids: list[UUID] = []
    cancel_requested_ids: list[UUID] = []

    for row in rows:
        if row.status in ("pending", "scheduled"):
            now = self._clock.now()
            self._jobs[row.id] = dc_replace(
                row,
                status="cancelled",
                finished_at=now,
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

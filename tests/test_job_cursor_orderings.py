"""A job ordering and the cursor that pages it are one object.

The rejection this replaces existed because ``JobFilter``'s orderings
tie-broke on ``id`` in the *opposite* direction to their primary column
(``created_at DESC, id ASC``).  A keyset cursor is a row-wise tuple
comparison, which can only express a page seam when every column of the
tuple sorts the same way, so no cursor could be written for those sorts
and the boundary refused them instead.

These are the codec-level pins.  End-to-end paging under every ordering,
on both backends, lives in ``test_backend_equivalence`` -- that is where
completeness (every job seen exactly once) is asserted.
"""

from datetime import UTC, datetime

import pytest

from taskq._ids import new_job_id
from taskq.backend._cursor import (
    decode_job_cursor,
    encode_cursor,
    encode_job_cursor,
    ordering_for,
)
from taskq.backend._protocol import JobSortField
from tests.test_backend_protocol import _make_job_row

_AT = datetime(2025, 6, 1, 12, 30, 45, 123456, tzinfo=UTC)

_ORDER_BYS: tuple[JobSortField | None, ...] = (None, *JobSortField)


@pytest.mark.parametrize("order_by", _ORDER_BYS)
def test_cursor_round_trips_to_the_columns_real_types(
    order_by: JobSortField | None,
) -> None:
    """Decoding must yield the columns' own Python types, never the text.

    asyncpg infers each placeholder's type from its ``::`` cast and
    refuses a ``str`` bound to ``timestamptz``/``uuid``/``int`` -- the
    ``DataError`` that 500'd every admin job-list page turn before
    2569da5.  Extending the cursor to the other orderings must not
    reintroduce it for them.
    """
    row = _make_job_row(
        id=new_job_id(), priority=7, created_at=_AT, scheduled_at=_AT, finished_at=_AT
    )
    ordering = ordering_for(order_by)

    values = decode_job_cursor(encode_job_cursor(row, order_by), order_by)

    assert values == ordering.values(row)
    assert not any(isinstance(v, str) for v in values), values


def test_default_ordering_cursor_matches_the_loose_argument_encoder() -> None:
    """``encode_cursor`` and the default ordering must render one format.

    Two entry points produce the default cursor -- this one, and
    ``JobOrdering.encode`` from a row.  Pinning them together is what
    stops the ordering's own encoding from drifting away from the
    callers that still hold three loose values.
    """
    row = _make_job_row(id=new_job_id(), priority=3, scheduled_at=_AT)

    assert encode_job_cursor(row, None) == encode_cursor(row.priority, row.scheduled_at, row.id)


def test_finished_at_cursor_carries_a_null_seam() -> None:
    """``finished_at DESC NULLS LAST`` pages through unfinished rows too.

    The seam between two NULL rows is a cursor whose leading field has no
    value; it must survive the round trip as ``None`` rather than as the
    string ``"None"``, which would then be bound against a
    ``timestamptz`` placeholder.
    """
    row = _make_job_row(id=new_job_id(), finished_at=None)

    values = decode_job_cursor(
        encode_job_cursor(row, JobSortField.FINISHED_AT_DESC), JobSortField.FINISHED_AT_DESC
    )

    assert values == (None, row.id)


@pytest.mark.parametrize("order_by", _ORDER_BYS)
def test_malformed_cursor_is_rejected(order_by: JobSortField | None) -> None:
    with pytest.raises(ValueError, match=r"[Cc]ursor"):
        decode_job_cursor("not-a-cursor", order_by)


def test_a_cursor_is_only_meaningful_under_its_own_ordering() -> None:
    """Cursor shapes are per-ordering and are not interchangeable.

    ``(created_at, id)`` has one field fewer than the default's
    ``(priority, scheduled_at, id)``.  Reading one as the other must
    fail at the codec rather than silently page against the wrong
    columns.
    """
    row = _make_job_row(id=new_job_id(), created_at=_AT)
    cursor = encode_job_cursor(row, JobSortField.CREATED_AT_DESC)

    with pytest.raises(ValueError, match=r"[Cc]ursor"):
        decode_job_cursor(cursor, None)

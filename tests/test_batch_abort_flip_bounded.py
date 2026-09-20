"""The abort-row flip is a bounded wait, not an unbounded one.

Livelock-hunt finding: ``abort_batch``'s batches-row flip ran as a bare
``conn.execute`` inside a plain transaction while the counter writes on
the very same row and the very same terminal-write path ran under
``_bounded_batches_row_wait``'s budgeted ``lock_timeout``. A streaming
append holding the batches row (its membership lock spans first chunk to
commit, unbounded) parked the flip, and the worker slot behind it, for
as long as the holder pleased. The flip now routes through the same
bounded wrapper; on expiry the abort is DELAYED (logged, nothing
written, the stale-batch sweep re-arbitrates) rather than parked.

Fake-conn pins (the test_batch*.py pattern, no PG):

* the flip under a held row times out bounded: ``abort_batch`` returns
  0, issues exactly one flip attempt, and never runs the member drain
  behind a still-'active' row (that is the completion race the
  flip-first order exists to close), and the delay is traceable in the
  log;
* the bounded wrapper's GUC discipline holds on both paths: inside a
  caller's transaction the prior ``lock_timeout`` is read and restored
  on success, and on the timeout path the restore is SKIPPED (the
  savepoint rollback already aborted that scope, any statement in it
  would fail);
* an uncontended flip is unchanged: it runs inside its own small
  transaction before the drain's first page.
"""

from __future__ import annotations

from uuid import UUID

import asyncpg
import structlog.testing

from taskq._ids import new_uuid
from taskq.backend._batch_sql import abort_batch, render_batch_sql

_FLIP_NEEDLE = "SET status = 'aborted'"
_PAGE_NEEDLE = "WITH matching AS MATERIALIZED"
_LOCK_TIMEOUT_READ = "current_setting('lock_timeout')"
_LOCK_TIMEOUT_SET = "set_config('lock_timeout'"

_BATCH_ID = new_uuid()
_SQL = render_batch_sql("taskq")


def _squeezed(sql: str) -> str:
    """Collapse whitespace: the templates carry multi-line comment blocks."""
    return " ".join(sql.split())


class _FakeTx:
    """The scope marker ``transaction()`` yields: counts each entry."""

    def __init__(self, conn: _AbortConn) -> None:
        self._conn = conn

    async def __aenter__(self) -> None:
        self._conn.transactions_opened += 1

    async def __aexit__(self, *args: object) -> None:
        return None


class _AbortConn:
    """asyncpg connection stand-in recording every awaited statement.

    ``flip_raises`` models a batches row held past the wrapper's
    ``lock_timeout``: the flip statement answers 55P03, everything else
    succeeds. ``page_rows`` feeds the drain's keyset pages, one dict per
    fetchrow, the matched/aborted/last_id aggregates the page statement
    returns.
    """

    def __init__(
        self,
        *,
        nested: bool,
        flip_raises: bool,
        page_rows: list[dict[str, object]] | None = None,
    ) -> None:
        self._nested = nested
        self._flip_raises = flip_raises
        self._page_rows = list(page_rows or [])
        self.executed: list[str] = []
        self.transactions_opened = 0

    def is_in_transaction(self) -> bool:
        return self._nested

    def transaction(self) -> _FakeTx:
        # Real shape: a bare connection gets a real short transaction, a
        # caller's transaction gets a savepoint; the fake models both as
        # a scope marker.
        return _FakeTx(self)

    async def fetchval(self, sql: str, *args: object) -> str | None:
        self.executed.append(_squeezed(sql))
        if _LOCK_TIMEOUT_READ in sql:
            # A caller-set prior budget, the value the wrapper restores.
            return "5s"
        return None

    async def execute(self, sql: str, *args: object) -> str:
        squeezed = _squeezed(sql)
        self.executed.append(squeezed)
        if _FLIP_NEEDLE in squeezed and self._flip_raises:
            raise asyncpg.LockNotAvailableError("simulated server lock_timeout")
        return "UPDATE 1"

    async def fetchrow(self, sql: str, *args: object) -> dict[str, object] | None:
        squeezed = _squeezed(sql)
        self.executed.append(squeezed)
        if _PAGE_NEEDLE in squeezed:
            if self._page_rows:
                return self._page_rows.pop(0)
            return {"matched_count": 0, "aborted_count": 0, "last_id": None}
        return None

    async def fetch(self, sql: str, *args: object) -> list[dict[str, object]]:
        # The batch statement_timeout capture (a GUC read), the drain's
        # only fetch use on this path.
        self.executed.append(_squeezed(sql))
        return [{"current_setting": "0"}]


async def test_flip_under_a_held_row_times_out_bounded_and_writes_nothing() -> None:
    """A batches row held past the budget: the abort returns 0 bounded,
    issues ONE flip attempt (no retry loop behind an unbounded holder),
    runs no member page (the drain must not proceed behind a
    still-'active' row), and logs the delay so it stays traceable."""
    conn = _AbortConn(nested=False, flip_raises=True)
    with structlog.testing.capture_logs() as captured:
        total = await abort_batch(conn, _SQL, _BATCH_ID)

    assert total == 0
    flips = [s for s in conn.executed if _FLIP_NEEDLE in s]
    assert len(flips) == 1, "bounded means one attempt, not a retry loop"
    assert not any(_PAGE_NEEDLE in s for s in conn.executed), (
        "the member drain must never run behind a skipped flip: its "
        "committed pages would be exposed to a concurrent completion "
        "arbiter, the race the flip-first order closes"
    )
    delay = [e for e in captured if e.get("event") == "batch-abort-row-lock-timeout"]
    assert [e["write"] for e in delay] == ["abort_row"], (
        "a delayed abort must be traceable to its cause"
    )
    assert [e["batch_id"] for e in delay] == [str(_BATCH_ID)]


async def test_flip_timeout_inside_a_caller_transaction_skips_the_guc_restore() -> None:
    """On the timeout path the prior ``lock_timeout`` is NOT restored: the
    savepoint's rollback already discarded the SET LOCAL, and the restore
    statement would land in an aborted scope (a raw 55P03 leaves the
    transaction failed even when caught). The wrapper's linear structure
    gives this for free; the pin keeps it that way."""
    conn = _AbortConn(nested=True, flip_raises=True)
    assert await abort_batch(conn, _SQL, _BATCH_ID) == 0

    reads = [s for s in conn.executed if _LOCK_TIMEOUT_READ in s]
    assert len(reads) == 1, "the caller's prior budget is captured before the set"
    sets = [s for s in conn.executed if _LOCK_TIMEOUT_SET in s]
    assert len(sets) == 1, (
        "the budget set once, never restored: the savepoint rollback "
        "aborted the scope the SET LOCAL lived in"
    )


async def test_uncontended_flip_inside_a_caller_transaction_restores_the_budget() -> None:
    """Success inside a caller's transaction: the flip runs in its own
    savepoint-scoped wait BEFORE any drain page, and the caller's prior
    ``lock_timeout`` is restored before the savepoint releases (a skipped
    restore would leak the wait bound onto every later statement)."""
    page = {
        "matched_count": 1,
        "aborted_count": 1,
        "last_id": UUID(int=1),
    }
    conn = _AbortConn(nested=True, flip_raises=False, page_rows=[page])
    total = await abort_batch(conn, _SQL, _BATCH_ID)

    assert total == 1
    reads = [s for s in conn.executed if _LOCK_TIMEOUT_READ in s]
    assert len(reads) == 1
    sets = [s for s in conn.executed if _LOCK_TIMEOUT_SET in s]
    assert len(sets) == 2, "budget set, then the caller's prior value restored"
    flip_idx = next(i for i, s in enumerate(conn.executed) if _FLIP_NEEDLE in s)
    page_idx = next(i for i, s in enumerate(conn.executed) if _PAGE_NEEDLE in s)
    assert flip_idx < page_idx, "the flip is FIRST: it decides abort-wins-over-complete"


async def test_uncontended_flip_on_an_owned_connection_sets_the_budget_once() -> None:
    """Success on a bare (owned) connection: a real short transaction, so
    there is no prior budget to read and no restore, exactly one GUC set
    around the flip."""
    conn = _AbortConn(nested=False, flip_raises=False)
    total = await abort_batch(conn, _SQL, _BATCH_ID)

    assert total == 0  # no members, the drain's first page is empty
    assert len([s for s in conn.executed if _LOCK_TIMEOUT_READ in s]) == 0
    assert len([s for s in conn.executed if _LOCK_TIMEOUT_SET in s]) == 1
    flips = [s for s in conn.executed if _FLIP_NEEDLE in s]
    assert len(flips) == 1

"""Docs must disclose that a caller-owned transaction holds
the `max_pending` advisory lock for its WHOLE lifetime, not just "the
duration of a contended enqueue".

`_enqueue_on_conn` (`src/taskq/backend/_enqueue.py`) documents, in its own
docstring, that "[a] caller who already holds a transaction owns the
scope — the advisory locks then span that caller's transaction, so
single-flight and cap exactness hold until its commit/rollback." That is
exactly the shape a transactional actor creates: `worker/_consumer.py`
runs the actor body inside an open transaction
(`async with transaction_conn.transaction():`), `default_start_to_close`
is `None` by default (unbounded actor runtime — see
`settings.py`), and a sub-enqueue made through `ctx.jobs` to a capped
actor joins that same open transaction. The `max_pending` advisory lock
it takes (`_advisory.acquire_advisory_xact_lock_bounded`,
`pg_advisory_xact_lock`) is therefore held until the *actor's* commit or
rollback — which can be the actor's entire (unbounded) runtime — not
just "the duration of a contended enqueue". Any OTHER producer to that
same capped actor queues behind the whole parent transaction and gets
the typed `MaxPendingLockTimeoutError` after the lock-timeout budget,
even when nowhere near the cap
(`tests/test_rt_locks_actor_tx_enqueue_serialization.py` pins that
runtime mechanism directly against Postgres).

The pins below hold `docs/guides/jobs-clients.md` to that mechanism: the
operational note must qualify "only for the duration of a contended
enqueue" with the caller-owned-transaction case, and the
`MaxPendingLockTimeoutError` error-table row must name a single
long-running transactional holder as a cause rather than attributing
every such timeout to "too many concurrent producers".
"""

from __future__ import annotations

from pathlib import Path

_DOCS = Path(__file__).resolve().parent.parent / "docs"
_SRC = Path(__file__).resolve().parent.parent / "src" / "taskq"


def test_max_pending_lock_docs_disclose_transactional_holder_duration() -> None:
    """The operational note must not claim the lock is scoped to "the
    duration of a contended enqueue" without qualifying that a caller-owned
    (e.g. transactional-actor) connection extends that duration to its own
    transaction lifetime."""
    text = (_DOCS / "guides" / "jobs-clients.md").read_text()
    section = text.split("Operational note: the lock exists only for capped actors", 1)
    assert len(section) == 2, "the operational note this test corrects has moved or been removed"
    note = section[1][:1200]

    # The unqualified claim must be gone: it is misleading whenever the
    # enqueue runs inside a caller-supplied open transaction (the
    # transactional-actor / ctx.jobs sub-enqueue shape).
    assert "only for the duration of a contended enqueue" not in note, (
        "docs still claim the lock is scoped to a single contended enqueue; this is "
        "false when the enqueue runs on a caller-owned open transaction (e.g. a "
        "transactional actor's ctx.jobs sub-enqueue), where the lock is held until "
        "that caller's commit/rollback — which can be the actor's entire runtime"
    )
    # The correction must be actionable: it must name the actual holder
    # (a caller-supplied/open transaction) and that its duration is not
    # bounded to the enqueue itself.
    assert "caller" in note and "transaction" in note, (
        "the corrected operational note must explain that an already-open caller "
        "transaction (e.g. a transactional actor's ctx.jobs sub-enqueue) extends "
        "the lock's hold duration to that transaction's own commit/rollback"
    )


def test_max_pending_lock_timeout_error_table_names_transactional_holder_cause() -> None:
    """The `MaxPendingLockTimeoutError` error-table row must not attribute
    the timeout ONLY to "too many concurrent producers" — a single
    long-running transactional holder (e.g. an unbounded actor sub-enqueuing
    via ctx.jobs) is a distinct, currently-undocumented cause."""
    text = (_DOCS / "guides" / "jobs-clients.md").read_text()
    marker = "| `MaxPendingLockTimeoutError` |"
    assert marker in text, (
        "the MaxPendingLockTimeoutError error-table row has moved or been removed"
    )
    row = text[text.index(marker) : text.index(marker) + 800]

    assert "too many concurrent producers, cap check never ran." not in row, (
        "the error-table row still attributes MaxPendingLockTimeoutError solely to "
        "concurrent-producer contention; it must also disclose that a single "
        "long-running caller transaction (e.g. a transactional actor's ctx.jobs "
        "sub-enqueue) can hold the lock for its whole (possibly unbounded) runtime "
        "and cause the same timeout for every other producer of that actor"
    )
    assert "transaction" in row, (
        "the corrected error-table row must mention the long-running-transaction cause"
    )


def test_in_memory_backend_max_pending_lacks_lock_semantics_is_documented() -> None:
    """The in-memory backend enforces `max_pending` with a plain in-process
    count and takes no lock at all (`src/taskq/testing/_enqueue.py`), so
    InMemoryBackend-based tests cannot reproduce (or catch a regression of)
    the transactional-holder lock-duration cost this issue describes. That
    parity gap must be disclosed somewhere docs readers of `max_pending` /
    `InMemoryBackend` testing guidance would see it."""
    enqueue_src = (_SRC / "testing" / "_enqueue.py").read_text()
    # Sanity: confirm the implementation still takes no actual advisory lock
    # (pg_advisory_xact_lock / acquire_advisory_xact_lock_bounded) in its
    # max_pending admission path -- this is the code-side half of the gap.
    # Bare mentions of "advisory" in comments (e.g. referencing the
    # taskq._advisory module convention) don't count; only a real acquire
    # call would close this gap.
    assert "acquire_advisory_xact_lock_bounded" not in enqueue_src, (
        "InMemoryBackend's max_pending path now calls the real advisory-lock "
        "acquirer; if it gained real lock semantics, update/remove this docs-parity "
        "contract"
    )
    assert "pg_advisory" not in enqueue_src, (
        "InMemoryBackend's max_pending path now references a real pg_advisory lock "
        "primitive; if it gained real lock semantics, update/remove this docs-parity "
        "contract"
    )

    docs_text = (_DOCS / "guides" / "jobs-clients.md").read_text()
    testing_docs_path = _DOCS / "guides" / "testing.md"
    haystacks = [docs_text]
    if testing_docs_path.exists():
        haystacks.append(testing_docs_path.read_text())

    # Co-occurrence in a *nearby* window, not just anywhere independently in
    # the whole document -- a doc that happens to mention "in-memory",
    # "max_pending" and "lock" in unrelated sections must not count as
    # disclosure. Windowed on each "in-memory"/"InMemoryBackend" mention.
    disclosed = False
    for text in haystacks:
        lowered = text.lower()
        start = 0
        while True:
            idx = lowered.find("in-memory", start)
            if idx == -1:
                idx = lowered.find("inmemorybackend", start)
            if idx == -1:
                break
            window = text[max(0, idx - 400) : idx + 400]
            window_lower = window.lower()
            if "max_pending" in window and (
                "no lock" in window_lower
                or "does not take" in window_lower
                or "takes no" in window_lower
                or "no advisory lock" in window_lower
                or ("in-process count" in window_lower and "lock" in window_lower)
            ):
                disclosed = True
                break
            start = idx + 1
        if disclosed:
            break

    assert disclosed, (
        "no docs guide discloses, near a mention of the in-memory backend, that "
        "InMemoryBackend's max_pending enforcement takes no advisory lock at all "
        "(plain in-process count), so it cannot reproduce the transactional-holder "
        "lock-duration cost real Postgres enqueues pay — a reader relying on "
        "InMemoryBackend-based tests to catch this class of regression has no "
        "warning that it won't"
    )

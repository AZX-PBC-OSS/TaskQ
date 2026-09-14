"""Tests for dedup behavioural outcomes on InMemoryBackend.

Covers:
  - unique_for dedup returns existing row
  - idempotency_key dedup returns existing row
  - fresh insert creates a new row (no dedup)
  - the dedup log lines match the PG path's unified contract:
    ``enqueue_deduplicated`` carrying ``status``, warning on a terminal
    target, info on a live one (PG side pinned in
    tests/test_silent_failure_guards.py)
  - the unique_for arm carries the same unified field set as the
    idempotency seam (``idempotency_scope`` included) and warns on a
    terminal target under caller-configured ``unique_states``
"""

from datetime import UTC, datetime, timedelta
from typing import Any

import structlog

from taskq._ids import new_job_id
from taskq.backend._protocol import EnqueueArgs, IdempotencyKey, IdentityKey, JobFilter
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend

_START = datetime(2025, 1, 1, tzinfo=UTC)


def _make_backend() -> InMemoryBackend:
    return InMemoryBackend(clock=FakeClock(_START))


def _keyed_args(key: IdempotencyKey) -> EnqueueArgs:
    """The repeated shape of the idempotency-key dedup pins below."""
    return EnqueueArgs(
        id=new_job_id(),
        actor="test_actor",
        queue="default",
        payload={"value": 1},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=_START,
        idempotency_key=key,
    )


def _unique_for_args(
    identity: IdentityKey,
    *,
    unique_states: tuple[str, ...] = ("pending", "scheduled", "running"),
) -> EnqueueArgs:
    """The repeated shape of the unique_for dedup pins below."""
    return EnqueueArgs(
        id=new_job_id(),
        actor="test_actor",
        queue="default",
        payload={"value": 1},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=_START,
        identity_key=identity,
        unique_for=timedelta(minutes=15),
        unique_states=unique_states,  # type: ignore[arg-type]  # Why: JobStatus is Literal[str, ...]; these pins pass the exact stored statuses
    )


#: Every field the unified ``enqueue_deduplicated`` line carries, on every
#: arm (idempotency_key and unique_for) and on both backends. One field set
#: is the observable of the shared helper: a site that re-implements the
#: dict inline drifts — the unique_for site omitted ``idempotency_scope``
#: until it was routed through the same helper as the idempotency seam.
_UNIFIED_DEDUP_FIELDS: frozenset[str] = frozenset(
    {
        "kind",
        "job_id",
        "actor",
        "queue",
        "identity_key",
        "idempotency_key",
        "idempotency_scope",
        "status",
        "existing_job_id",
        "dedup_reason",
    }
)


def _sole_dedup_line(captured: list[dict[str, Any]]) -> dict[str, Any]:
    """The single ``enqueue_deduplicated`` line from a captured log run."""
    dedup_lines = [e for e in captured if e.get("event") == "enqueue_deduplicated"]
    assert dedup_lines, f"no dedup log line emitted; captured={captured}"
    return dedup_lines[0]


async def test_unique_for_dedup_returns_existing_row() -> None:
    backend = _make_backend()
    identity = IdentityKey("account:99")

    row1 = await backend.enqueue(
        EnqueueArgs(
            id=new_job_id(),
            actor="test_actor",
            queue="default",
            payload={"value": 1},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=_START,
            identity_key=identity,
            unique_for=timedelta(minutes=15),
            unique_states=("pending", "scheduled", "running"),
        )
    )

    row2 = await backend.enqueue(
        EnqueueArgs(
            id=new_job_id(),
            actor="test_actor",
            queue="default",
            payload={"value": 1},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=_START,
            identity_key=identity,
            unique_for=timedelta(minutes=15),
            unique_states=("pending", "scheduled", "running"),
        )
    )

    assert row1.id == row2.id


async def test_idempotency_key_dedup_returns_existing_row() -> None:
    backend = _make_backend()
    key = IdempotencyKey("dedup-key-1")

    row1 = await backend.enqueue(
        EnqueueArgs(
            id=new_job_id(),
            actor="test_actor",
            queue="default",
            payload={"value": 1},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=_START,
            idempotency_key=key,
        )
    )

    row2 = await backend.enqueue(
        EnqueueArgs(
            id=new_job_id(),
            actor="test_actor",
            queue="default",
            payload={"value": 1},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=_START,
            idempotency_key=key,
        )
    )

    assert row1.id == row2.id


async def test_fresh_insert_creates_new_row() -> None:
    backend = _make_backend()

    row = await backend.enqueue(
        EnqueueArgs(
            id=new_job_id(),
            actor="test_actor",
            queue="default",
            payload={"value": 1},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=_START,
            identity_key=IdentityKey("account:42"),
            unique_for=timedelta(minutes=15),
        )
    )

    assert row.id is not None


# ── dedup log parity with the PG path ────────────────────────────────────


async def test_idempotency_dedup_onto_terminal_job_warns_with_status() -> None:
    """A dedup hit whose target job is terminal must warn and name its status.

    The PG contract is pinned in tests/test_silent_failure_guards.py
    (TestTerminalDedupIsObservable); the mirror must emit the same
    ``enqueue_deduplicated`` event, carry the same ``status`` field, and be
    louder for a dead target than for a live one.
    """
    backend = _make_backend()
    key = IdempotencyKey("terminal-dedup-mem")

    first = await backend.enqueue(_keyed_args(key))
    cancelled = await backend.cancel_where(JobFilter(actor="test_actor"), reason="dedup-log-pin")
    assert cancelled.cancelled_ids == (first.id,), (
        "precondition: the target job must be terminal, or the pin asserts nothing"
    )

    with structlog.testing.capture_logs() as captured:
        second = await backend.enqueue(_keyed_args(key))

    assert second.id == first.id, "precondition: the terminal row still dedupes"

    line = _sole_dedup_line(captured)
    assert line.get("status") == "cancelled", (
        f"dedup onto a terminal job must record the target's status; got {line!r}"
    )
    assert line.get("log_level") == "warning", (
        "dedup onto a terminal job must be louder than a live-job hit; "
        f"got log_level={line.get('log_level')!r}"
    )


async def test_idempotency_dedup_onto_live_job_stays_info() -> None:
    """A dedup hit on a still-pending job is normal single-flight operation
    and must stay at info — carrying the status on every hit, not only
    terminal ones."""
    backend = _make_backend()
    key = IdempotencyKey("live-dedup-mem")

    first = await backend.enqueue(_keyed_args(key))

    with structlog.testing.capture_logs() as captured:
        second = await backend.enqueue(_keyed_args(key))

    assert second.id == first.id, "precondition: the live row dedupes"

    line = _sole_dedup_line(captured)
    assert line.get("log_level") == "info", (
        f"dedup onto a live job must stay at info; got log_level={line.get('log_level')!r}"
    )
    assert line.get("status") == "pending", (
        "the dedup line must carry the target's status on every hit, "
        f"not only terminal ones; got {line!r}"
    )


async def test_unique_for_dedup_line_matches_the_unified_contract() -> None:
    """The unique_for arm emits the same unified ``enqueue_deduplicated``
    event, carrying the target's status at info — matching the PG path.
    With the default ``unique_states`` the preflight only matches active
    rows, so a hit is normal single-flight operation; the
    terminal-target case (custom ``unique_states``) has its own pin
    below and must warn like the idempotency seam."""
    backend = _make_backend()
    identity = IdentityKey("account:7")

    first = await backend.enqueue(_unique_for_args(identity))

    with structlog.testing.capture_logs() as captured:
        second = await backend.enqueue(_unique_for_args(identity))

    assert second.id == first.id, "precondition: the unique_for row dedupes"

    line = _sole_dedup_line(captured)
    assert line.get("log_level") == "info"
    assert line.get("status") == "pending", f"dedup line missing status; got {line!r}"
    assert line.get("dedup_reason") == "unique_for"


async def test_unique_for_dedup_line_carries_the_full_field_set() -> None:
    """The unique_for arm's line carries the same fields as the
    idempotency seam's — one field set, per-site ``dedup_reason`` only.

    A site that builds its own field dict drifts from the shared
    contract: the unique_for site omitted ``idempotency_scope`` while
    the idempotency seam carried it, so a log query keyed on the pair
    silently missed every unique_for dedup.
    """
    backend = _make_backend()
    identity = IdentityKey("account:8")

    first = await backend.enqueue(_unique_for_args(identity))

    with structlog.testing.capture_logs() as captured:
        second = await backend.enqueue(_unique_for_args(identity))

    assert second.id == first.id, "precondition: the unique_for row dedupes"

    line = _sole_dedup_line(captured)
    missing = _UNIFIED_DEDUP_FIELDS - set(line)
    assert not missing, (
        f"the unique_for dedup line must carry the unified field set; "
        f"missing {sorted(missing)}; got {line!r}"
    )


async def test_unique_for_dedup_onto_terminal_target_warns_with_status() -> None:
    """A unique_for dedup whose target is TERMINAL must warn and name its
    status — mirroring the idempotency seam's terminal-target pin above.

    The default ``unique_states`` excludes terminal states, but the set
    is caller-configurable (``@actor(unique_states=...)``), and a window
    that folds a terminal state in pins the identity to a dead job for
    the whole window: the enqueue silently returns success while no work
    will ever run. That is exactly the case the idempotency seam already
    warns for, so the unique_for arm must be at least as loud.
    """
    backend = _make_backend()
    identity = IdentityKey("account:9")
    states_including_terminal = ("pending", "scheduled", "running", "cancelled")

    first = await backend.enqueue(
        _unique_for_args(identity, unique_states=states_including_terminal)
    )
    cancelled = await backend.cancel_where(JobFilter(actor="test_actor"), reason="dedup-log-pin")
    assert cancelled.cancelled_ids == (first.id,), (
        "precondition: the target job must be terminal, or the pin asserts nothing"
    )

    with structlog.testing.capture_logs() as captured:
        second = await backend.enqueue(
            _unique_for_args(identity, unique_states=states_including_terminal)
        )

    assert second.id == first.id, "precondition: the terminal row still dedupes"

    line = _sole_dedup_line(captured)
    assert line.get("status") == "cancelled", (
        f"dedup onto a terminal job must record the target's status; got {line!r}"
    )
    assert line.get("log_level") == "warning", (
        "dedup onto a terminal job must be louder than a live-job hit; "
        f"got log_level={line.get('log_level')!r}"
    )
    assert line.get("dedup_reason") == "unique_for"

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

import pytest
import structlog
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

import taskq.obs as obs_mod
import taskq.obs._otel as otel_mod
from taskq._ids import new_job_id
from taskq.backend._protocol import EnqueueArgs, IdempotencyKey, IdentityKey, JobFilter
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend

_START = datetime(2025, 1, 1, tzinfo=UTC)


@pytest.fixture
def otel_reader(monkeypatch: pytest.MonkeyPatch) -> InMemoryMetricReader:
    """Per-test OTel meter isolation.

    Patches ``obs._otel.get_meter`` onto a fresh MeterProvider backed by an
    ``InMemoryMetricReader``, so instruments created lazily by the code
    under test land in this reader.  Mirrors ``tests/test_obs.py`` and
    ``tests/test_denial_observability.py``.
    """
    from opentelemetry.sdk.metrics import MeterProvider

    reader = InMemoryMetricReader()
    new_provider = MeterProvider(metric_readers=[reader])
    new_meter = new_provider.get_meter(obs_mod.INSTRUMENTATION_NAME, otel_mod._version())  # pyright: ignore[reportPrivateUsage]  # Why: mirrors tests/test_obs.py's otel_reader fixture, which reads the same private version helper.

    monkeypatch.setattr(otel_mod, "get_meter", lambda: new_meter)
    monkeypatch.setattr(obs_mod, "get_meter", lambda: new_meter)
    otel_mod.set_otel_enabled(True)
    return reader


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


# ── the dedup rate signal: taskq.enqueue.dedups ──────────────────────────


def _dedup_points_by_reason(
    reader: InMemoryMetricReader,
) -> dict[str, int]:
    """The ``taskq.enqueue.dedups`` counter's value per dedup_reason label."""
    from taskq.testing.otel import counter_data_points

    points = counter_data_points(reader, "taskq.enqueue.dedups")
    return {
        str(dict(p.attributes or {}).get("dedup_reason")): int(p.value) for p in points
    }


async def test_idempotency_dedup_hit_counts_on_enqueue_dedups(otel_reader: InMemoryMetricReader) -> None:
    """An idempotency-key dedup hit bumps ``taskq.enqueue.dedups`` once,
    labeled by the bounded ``dedup_reason`` enum value ``idempotency_key``.

    The dedup log lines are per-hit observability an operator reads one at
    a time; a stampede's RATE needs a counter. Both backends share the
    dedup-report helper, so the mirror's hit pins the shared emission.
    """
    backend = _make_backend()
    key = IdempotencyKey("dedup-counter-mem")

    first = await backend.enqueue(_keyed_args(key))
    second = await backend.enqueue(_keyed_args(key))

    assert second.id == first.id, "precondition: the hit dedupes"
    assert _dedup_points_by_reason(otel_reader) == {"idempotency_key": 1}, (
        "an idempotency dedup hit must land on taskq.enqueue.dedups with "
        "dedup_reason='idempotency_key' — a dedup stampede has no rate signal "
        "otherwise"
    )


async def test_unique_for_dedup_hit_counts_on_enqueue_dedups(otel_reader: InMemoryMetricReader) -> None:
    """A unique_for dedup hit lands on the same counter under its own
    ``dedup_reason`` — the two reasons a dedup hit can occur are the whole
    label set, and neither arm may be the uncounted one."""
    backend = _make_backend()
    identity = IdentityKey("account:11")

    first = await backend.enqueue(_unique_for_args(identity))
    second = await backend.enqueue(_unique_for_args(identity))

    assert second.id == first.id, "precondition: the hit dedupes"
    assert _dedup_points_by_reason(otel_reader) == {"unique_for": 1}, (
        "a unique_for dedup hit must land on taskq.enqueue.dedups with "
        "dedup_reason='unique_for' — the identity-pinned-to-dead-job stampede "
        "is exactly the case that needs a rate signal"
    )


async def test_fresh_enqueue_emits_no_dedup_counter_datapoint(
    otel_reader: InMemoryMetricReader,
) -> None:
    """A fresh insert is not a dedup: the counter must carry no datapoint,
    so the counter's rate is a pure dedup rate, not an enqueue rate."""
    backend = _make_backend()

    await backend.enqueue(_keyed_args(IdempotencyKey("fresh-counter-mem")))
    await backend.enqueue(_unique_for_args(IdentityKey("account:12")))

    assert _dedup_points_by_reason(otel_reader) == {}, (
        "fresh inserts must not touch taskq.enqueue.dedups — a counter that "
        "also counts fresh enqueues cannot serve as a dedup rate signal"
    )


async def test_batch_dedup_stampede_counts_every_hit_despite_warning_suppression(
    otel_reader: InMemoryMetricReader,
) -> None:
    """A terminal-target dedup stampede at batch scale counts EVERY hit on
    the counter even where the per-hit WARNING budget suppresses the log
    lines — the counter is the rate signal that survives the flood bound.

    The WARNING budget (``#140``) bounds per-hit terminal WARNINGs to
    three plus one summary line; a stampede that once emitted 500
    WARNINGs now emits four. What it must NOT lose is the rate itself:
    the operator muting nothing still needs to see 500 dedups happened.
    """
    from taskq.backend._enqueue import _DEDUP_WARN_PER_HIT_LIMIT  # pyright: ignore[reportPrivateUsage]  # Why: the bound under attack is the production constant; redefining it here would let the pin drift from the budget that actually runs.

    backend = _make_backend()
    n_items = _DEDUP_WARN_PER_HIT_LIMIT + 9  # comfortably past the per-hit budget
    keys = [IdempotencyKey(f"stampede-{i:02d}") for i in range(n_items)]
    seed_rows = await backend.enqueue_batch([_keyed_args(key) for key in keys])
    cancelled = await backend.cancel_where(JobFilter(actor="test_actor"), reason="counter-pin")
    assert len(cancelled.cancelled_ids) == n_items, (
        "precondition: every target must be terminal, or the stampede arm never runs"
    )

    with structlog.testing.capture_logs() as captured:
        dedup_rows = await backend.enqueue_batch([_keyed_args(key) for key in keys])

    assert [row.id for row in dedup_rows] == [row.id for row in seed_rows], (
        "precondition: every re-enqueued item must dedup onto its seeded row"
    )
    warnings = [
        e
        for e in captured
        if e.get("event") == "enqueue_deduplicated" and e.get("log_level") == "warning"
    ]
    assert len(warnings) <= _DEDUP_WARN_PER_HIT_LIMIT + 1, (
        f"precondition: the WARNING flood bound must hold ({len(warnings)} lines) — "
        "this pin is about the counter surviving the bound, not re-litigating it"
    )
    assert _dedup_points_by_reason(otel_reader) == {"idempotency_key": n_items}, (
        f"a {n_items}-item dedup stampede must count {n_items} on "
        "taskq.enqueue.dedups even when the per-hit WARNINGs are suppressed — "
        "after the flood bound, the counter IS the rate signal"
    )

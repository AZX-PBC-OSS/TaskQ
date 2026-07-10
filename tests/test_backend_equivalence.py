"""Equivalence harness — parametrised tests via ``backend_pair``.

Asserts both backends produce the same final state for the same scenario.
Both InMemoryBackend and PostgresBackend (via testcontainers) are exercised
through the ``backend_pair`` fixture which parametrises over ``["memory",
"pg"]``.
"""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest

from taskq._ids import new_job_id, new_uuid
from taskq.backend import (
    AttemptOutcome,
    AttemptRow,
    Backend,
    EnqueueArgs,
    JobFilter,
    JobRow,
)
from taskq.backend._protocol import ErrorInfo, EventRow, IdentityKey, JobId
from taskq.testing.in_memory import InMemoryBackend, encode_cursor

# The harness exercises PG via backend_pair; PG branch must be opt-in.
pytestmark = pytest.mark.integration

# ── Helpers ────────────────────────────────────────────────────────────

_START = datetime(2025, 1, 1, tzinfo=UTC)
_LOCK_LEASE = timedelta(seconds=60)

_ACTORS = ["actor_a", "actor_b", "actor_c"]
_QUEUES = ["default", "high", "low"]


def _now_for(backend: Backend) -> datetime:
    """Return the current time for the backend's clock.

    InMemoryBackend uses a FakeClock starting at ``_START``;
    PostgresBackend uses server-side ``now()`` approximated by
    ``datetime.now(UTC)``.
    """
    if isinstance(backend, InMemoryBackend):
        return backend._clock.now()  # type: ignore[reportPrivateUsage] # Why: equivalence test helper; _clock is the canonical time source for InMemoryBackend
    return datetime.now(UTC)


async def _ensure_worker(backend: Backend) -> UUID:
    """Return a worker_id that exists in the ``workers`` table.

    InMemoryBackend: returns the internal ``_worker_id``.
    PostgresBackend: inserts a row into ``\"{schema}\".workers`` and returns it.
    """
    if isinstance(backend, InMemoryBackend):
        return backend._worker_id  # type: ignore[reportPrivateUsage] # Why: equivalence test helper; _worker_id is the canonical worker identity for InMemoryBackend
    import asyncpg

    schema: str = backend._schema_name  # type: ignore[reportPrivateUsage] # Why: PG-path helper mirrors existing _pg_enqueue_dispatch pattern
    pool: asyncpg.Pool = backend._worker_pool  # type: ignore[reportPrivateUsage] # Why: same pattern
    worker_id = new_uuid()
    async with pool.acquire() as conn:  # type: ignore[reportUnknownVariableType] # Why: asyncpg stubs yield PoolConnectionProxy | Unknown
        await conn.execute(
            f'INSERT INTO "{schema}".workers (id, hostname, pid, queues) VALUES ($1, $2, $3, $4)',
            worker_id,
            "test-host",
            12345,
            ["default"],
        )
    return worker_id


async def _force_job_state(backend: Backend, job_id: JobId, **overrides: object) -> None:
    """Force job row fields, bypassing normal state-machine transitions.

    InMemoryBackend: uses ``dataclasses.replace`` on ``_jobs[job_id]``.
    PostgresBackend: direct ``UPDATE \"{schema}\".jobs SET … WHERE id = $1``.
    Handles the ``status`` enum column with an explicit PG cast.
    """
    if isinstance(backend, InMemoryBackend):
        row = backend._jobs[job_id]  # type: ignore[reportPrivateUsage] # Why: equivalence test helper; _jobs is the canonical store for InMemoryBackend
        backend._jobs[job_id] = replace(row, **overrides)  # type: ignore[reportPrivateUsage] # Why: same
        return
    import asyncpg

    schema: str = backend._schema_name  # type: ignore[reportPrivateUsage] # Why: PG-path helper mirrors existing _pg_enqueue_dispatch pattern
    pool: asyncpg.Pool = backend._worker_pool  # type: ignore[reportPrivateUsage] # Why: same pattern
    sets: list[str] = []
    vals: list[object] = []
    for i, (k, v) in enumerate(overrides.items()):
        param_idx = i + 2
        if k == "status":
            # ``status`` is a PG enum (\"{schema}\".job_status); needs explicit cast.
            sets.append(f'{k} = ${param_idx}::"{schema}".job_status')
        else:
            sets.append(f"{k} = ${param_idx}")
        vals.append(v)
    sql_sets = ", ".join(sets)
    async with pool.acquire() as conn:  # type: ignore[reportUnknownVariableType] # Why: asyncpg stubs
        await conn.execute(
            f'UPDATE "{schema}".jobs SET {sql_sets} WHERE id = $1',
            job_id,
            *vals,
        )


async def _advance_and_promote(backend: Backend, target_time: datetime) -> int:
    """Advance to *target_time* and promote scheduled jobs to pending.

    InMemoryBackend: advances the FakeClock and calls ``scheduled_to_pending``.
    PostgresBackend: forces all scheduled jobs' ``scheduled_at`` to the past
    (so server-side ``now()`` will find them eligible) and calls
    ``scheduled_to_pending``.
    """
    if isinstance(backend, InMemoryBackend):
        backend.advance_clock_to(target_time)
        return await backend.scheduled_to_pending(target_time)
    # PG: server-side now() controls promotion; force scheduled_at to the past
    # so that scheduled_to_pending(now()) will promote the job.
    import asyncpg

    schema: str = backend._schema_name  # type: ignore[reportPrivateUsage] # Why: PG-path helper
    pool: asyncpg.Pool = backend._worker_pool  # type: ignore[reportPrivateUsage] # Why: same pattern
    async with pool.acquire() as conn:  # type: ignore[reportUnknownVariableType] # Why: asyncpg stubs
        # 5s margin, not 1s: scheduled_at is set from the PG server clock
        # while scheduled_to_pending's threshold below comes from the Python
        # client clock (datetime.now(UTC)) — these can differ by several
        # hundred ms under this environment's connection-pool/scheduling
        # characteristics (see test_heartbeat_integration.py's
        # _CLOCK_JITTER_TOLERANCE), which a 1s margin doesn't reliably absorb.
        await conn.execute(
            f"UPDATE \"{schema}\".jobs SET scheduled_at = now() - interval '5 seconds' "
            f"WHERE status = 'scheduled'"
        )
    return await backend.scheduled_to_pending(datetime.now(UTC))


async def _get_events(backend: Backend, job_id: JobId) -> list[EventRow]:
    """Get events for a job across both backends.

    InMemoryBackend.get_events and PostgresBackend.get_events are both
    async. This helper awaits correctly for both.
    """
    return await backend.get_events(job_id)


def _worker_id_of(backend: Backend) -> UUID:
    return backend._worker_id  # type: ignore[reportPrivateUsage] # Why: equivalence tests are owned helpers; _worker_id is the canonical worker identity for InMemoryBackend dispatch


async def _enqueue_dispatch(
    backend: Backend,
    *,
    actor: str = "actor_a",
    queue: str = "default",
    max_attempts: int = 3,
    retry_kind: str = "transient",
    schedule_to_close: datetime | None = None,
    priority: int = 0,
) -> tuple[JobId, UUID]:
    """Enqueue a job and dispatch it, returning (job_id, worker_id)."""
    job_id = new_job_id()
    await backend.enqueue(
        EnqueueArgs(
            id=job_id,
            actor=actor,
            queue=queue,
            payload={"key": "value"},
            max_attempts=max_attempts,
            retry_kind=retry_kind,  # type: ignore[arg-type] # Why: retry_kind is str param from callers; validated as RetryKind at runtime by the backend.
            scheduled_at=_START,
            schedule_to_close=schedule_to_close,
            priority=priority,
        )
    )
    worker_id = _worker_id_of(backend)
    dispatched = await backend.dispatch_batch(
        worker_id=worker_id,
        queues=[queue],
        limit=1,
        lock_lease=_LOCK_LEASE,
    )
    assert len(dispatched) == 1
    return job_id, worker_id


def _assert_job_row(
    row: JobRow,
    *,
    status: str,
    attempt: int | None = None,
    error_class: str | None = None,
    error_message: str | None = None,
    locked: bool = False,
    last_heartbeat_at_none: bool = False,
) -> None:
    assert row.status == status
    if attempt is not None:
        assert row.attempt == attempt
    if error_class is not None:
        assert row.error_class == error_class
    if error_message is not None:
        assert row.error_message == error_message
    if not locked:
        assert row.locked_by_worker is None
        assert row.lock_expires_at is None
    if last_heartbeat_at_none:
        assert row.last_heartbeat_at is None


def _assert_attempt_row(
    attempts: list[AttemptRow],
    index: int,
    *,
    outcome: AttemptOutcome,
    error_class: str | None = None,
    error_message: str | None = None,
) -> None:
    assert attempts[index].outcome == outcome
    if error_class is not None:
        assert attempts[index].error_class == error_class
    if error_message is not None:
        assert attempts[index].error_message == error_message


def _assert_state_change_event(
    events: list[EventRow],
    from_state: str,
    to_state: str,
    error_class: str | None = None,
) -> None:
    state_changes = [e for e in events if e.kind == "state_change"]
    matching = [
        e
        for e in state_changes
        if e.detail.get("from_state") == from_state and e.detail.get("to_state") == to_state
    ]
    assert len(matching) >= 1, f"no state_change {from_state}→{to_state} found"
    if error_class is not None:
        assert matching[0].detail.get("error_class") == error_class


async def _set_actor_cap(
    backend: Backend,
    *,
    actor: str,
    max_concurrent: int | None = None,
    queue: str = "default",
) -> None:
    """Configure actor cap consistently across backends.

    InMemoryBackend: uses ``register_actor_config``.
    PostgresBackend: inserts directly into ``\"{schema}\".actor_config`` via
    ``dispatcher_pool``.
    """
    if isinstance(backend, InMemoryBackend):
        backend.register_actor_config(actor=actor, max_concurrent=max_concurrent, queue=queue)
        return

    import asyncpg

    schema: str = backend._schema_name  # type: ignore[reportPrivateUsage] # Why: PG-path helper mirrors existing _pg_enqueue_dispatch pattern
    pool: asyncpg.Pool | None = backend._dispatcher_pool  # type: ignore[reportPrivateUsage] # Why: same pattern
    assert pool is not None, "dispatcher_pool required for PG actor_config insert"
    async with pool.acquire() as conn:  # type: ignore[reportUnknownVariableType] # Why: asyncpg stubs yield PoolConnectionProxy | Unknown
        await conn.execute(
            f"""INSERT INTO \"{schema}\".actor_config (actor, max_concurrent, queue, metadata)
                VALUES ($1, $2, $3, '{{}}'::jsonb)
                ON CONFLICT (actor) DO UPDATE SET max_concurrent = $2, queue = $3""",
            actor,
            max_concurrent,
            queue,
        )


# ── mass enqueue determinism ────────────────────────────────────


async def test_mass_enqueue_sort_order(backend_pair: Backend) -> None:
    """enqueue 50 jobs with unique priorities into the backend.
    Call dispatch_batch(limit=10). Assert the returned job_id list is
    sorted by (priority DESC, scheduled_at ASC).

    Uses unique priorities (shuffled 0-49) so the dispatch sort key
    ``(priority DESC, scheduled_at)`` is unambiguous — PG dispatch SQL
    sorts by priority DESC, scheduled_at without an id tie-breaker,
    while InMemory adds id. With unique priorities both agree.
    """
    import random

    priorities = list(range(50))
    rng = random.Random(42)  # noqa: S311
    rng.shuffle(priorities)
    ids = [new_job_id() for _ in range(50)]

    # Enqueue all 50 with identical scheduled_at (all become pending)
    for i, (jid, pri) in enumerate(zip(ids, priorities, strict=True)):
        await backend_pair.enqueue(
            EnqueueArgs(
                id=jid,
                actor=_ACTORS[i % len(_ACTORS)],
                queue=_QUEUES[i % len(_QUEUES)],
                payload={"i": i},
                max_attempts=3,
                retry_kind="transient",
                scheduled_at=_START,
                priority=pri,
            )
        )

    # Ensure actor_config rows exist for all actors (required by per_actor_capacity CTE)
    for actor in _ACTORS:
        await _set_actor_cap(backend_pair, actor=actor)  # uncapped

    # Dispatch batch of 10 (use explicit queues — PG requires non-empty
    # queue list; InMemory treats empty as "all").
    wid = await _dispatch_worker_id(backend_pair)
    dispatched = await backend_pair.dispatch_batch(
        worker_id=wid,
        queues=_QUEUES,
        limit=10,
        lock_lease=_LOCK_LEASE,
    )

    assert len(dispatched) == 10
    dispatched_ids = [row.id for row in dispatched]

    for did in dispatched_ids:
        assert did in ids, f"Dispatched unknown job id {did}"

    # Invariant: within each actor, dispatched jobs maintain priority DESC order
    id_to_pri = {ids[i]: priorities[i] for i in range(50)}
    id_to_actor = {ids[i]: _ACTORS[i % len(_ACTORS)] for i in range(50)}
    per_actor: dict[str, list[int]] = {}
    for did in dispatched_ids:
        per_actor.setdefault(id_to_actor[did], []).append(id_to_pri[did])
    for actor, pris in per_actor.items():
        for i in range(len(pris) - 1):
            assert pris[i] >= pris[i + 1], (
                f"Actor {actor}: priority {pris[i]} before {pris[i + 1]} violates DESC order"
            )
            break  # Only need to check first rank-2

    # Invariant: within same actor, priority DESC
    per_actor_dispatched: dict[str, list[int]] = {}
    for did in dispatched_ids:
        actor = id_to_actor[did]
        per_actor_dispatched.setdefault(actor, []).append(id_to_pri[did])
    for actor, pris in per_actor_dispatched.items():
        for i in range(len(pris) - 1):
            assert pris[i] >= pris[i + 1], (
                f"Actor {actor}: priority {pris[i]} before {pris[i + 1]} violates DESC order"
            )


# ── write_attempt / get_attempts round-trip ─────────────────────


async def test_write_attempt_get_attempts_roundtrip(backend_pair: Backend) -> None:
    """enqueue, simulate dispatch, write two AttemptRows.
    Assert get_attempts returns them in attempt order with correct fields.
    """
    from taskq.backend import AttemptRow

    job_id = new_job_id()
    await backend_pair.enqueue(
        EnqueueArgs(
            id=job_id,
            actor="actor_a",
            queue="default",
            payload={"x": 1},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=_START,
        )
    )

    # Ensure a worker row exists (PG job_attempts.worker_id has FK to workers)
    worker_id = await _ensure_worker(backend_pair)

    # Write two attempts
    await backend_pair.write_attempt(
        AttemptRow(
            job_id=job_id,
            attempt=1,
            started_at=_START,
            finished_at=_START + timedelta(seconds=5),
            outcome="failed",
            error_class="ValueError",
            error_message="boom",
            error_traceback=None,
            duration_ms=5000,
            worker_id=worker_id,
            metadata={},
        )
    )
    await backend_pair.write_attempt(
        AttemptRow(
            job_id=job_id,
            attempt=2,
            started_at=_START + timedelta(seconds=5),
            finished_at=_START + timedelta(seconds=10),
            outcome="succeeded",
            error_class=None,
            error_message=None,
            error_traceback=None,
            duration_ms=5000,
            worker_id=worker_id,
            metadata={},
        )
    )

    attempts = await backend_pair.get_attempts(job_id)
    assert len(attempts) == 2
    assert attempts[0].attempt == 1
    assert attempts[1].attempt == 2
    assert attempts[0].outcome == "failed"
    assert attempts[1].outcome == "succeeded"
    assert attempts[0].error_class == "ValueError"
    assert attempts[1].error_class is None


# ── scheduled_to_pending equivalence ────────────────────────────


async def test_scheduled_to_pending_equivalence(backend_pair: Backend) -> None:
    """enqueue with scheduled_at in the future. dispatch_batch returns
    empty. Call scheduled_to_pending. dispatch_batch returns the job.
    """
    job_id = new_job_id()
    scheduled_at = _now_for(backend_pair) + timedelta(hours=1)
    await backend_pair.enqueue(
        EnqueueArgs(
            id=job_id,
            actor="actor_a",
            queue="default",
            payload={},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=scheduled_at,
        )
    )

    # For PG: enqueue defaults status to 'pending'; force to 'scheduled'
    # so that scheduled_to_pending has something to promote.
    await _force_job_state(backend_pair, job_id, status="scheduled")

    # Not yet due → dispatch returns empty
    wid = await _dispatch_worker_id(backend_pair)
    dispatched = await backend_pair.dispatch_batch(
        worker_id=wid,
        queues=["default"],
        limit=1,
        lock_lease=_LOCK_LEASE,
    )
    assert dispatched == []

    # Advance/promote scheduled jobs
    count = await _advance_and_promote(backend_pair, scheduled_at + timedelta(hours=1))
    assert count == 1

    # Now dispatch succeeds
    dispatched = await backend_pair.dispatch_batch(
        worker_id=wid,
        queues=["default"],
        limit=1,
        lock_lease=_LOCK_LEASE,
    )
    assert len(dispatched) == 1
    assert dispatched[0].id == job_id
    assert dispatched[0].status == "running"


# ── list_jobs cursor pagination equivalence ────────────────────


async def test_list_jobs_cursor_pagination(backend_pair: Backend) -> None:
    """enqueue 5 jobs with varying priorities. Call list_jobs(limit=2)
    — assert both backends return the same 2 job ids. Use the cursor from
    the last row; call list_jobs(limit=2, cursor=cursor) — assert both
    return the same next 2 job ids. Validates keyset pagination.
    """
    # Enqueue 5 jobs with different priorities
    priorities = [5, 10, 3, 8, 1]
    ids = [new_job_id() for _ in range(5)]
    for jid, pri in zip(ids, priorities, strict=True):
        await backend_pair.enqueue(
            EnqueueArgs(
                id=jid,
                actor="actor_a",
                queue="default",
                payload={"pri": pri},
                max_attempts=3,
                retry_kind="transient",
                scheduled_at=_START,
                priority=pri,
            )
        )

    # First page
    page1 = await backend_pair.list_jobs(JobFilter(limit=2))
    assert len(page1) == 2
    page1_ids = [r.id for r in page1]

    # Expected order: priority 10 then 8 (highest first)
    assert page1_ids[0] == ids[1]  # priority 10
    assert page1_ids[1] == ids[3]  # priority 8

    # Cursor from last row of page 1
    last = page1[-1]
    cursor = encode_cursor(last.priority, last.scheduled_at, last.id)

    # Second page
    page2 = await backend_pair.list_jobs(JobFilter(limit=2, cursor=cursor))
    assert len(page2) == 2
    page2_ids = [r.id for r in page2]

    # Expected order: priority 5 then 3
    assert page2_ids[0] == ids[0]  # priority 5
    assert page2_ids[1] == ids[2]  # priority 3

    # Cursor from last row of page 2
    last2 = page2[-1]
    cursor2 = encode_cursor(last2.priority, last2.scheduled_at, last2.id)

    # Third page — only 1 job left
    page3 = await backend_pair.list_jobs(JobFilter(limit=2, cursor=cursor2))
    assert len(page3) == 1
    assert page3[0].id == ids[4]  # priority 1


# ── PG backend raises typed exception on connection loss ────────


async def test_pg_connection_loss_raises_typed_exception(
    backend_pair: Backend,
    request: pytest.FixtureRequest,
) -> None:
    """PG backend raises typed exception on connection loss."""
    # Only applies to PG backend; memory backend skips this path.
    if not hasattr(backend_pair, "_worker_pool"):
        pytest.skip("connection-loss test requires PostgresBackend")

    pool = backend_pair._worker_pool  # type: ignore[reportPrivateUsage,union-attr] # Why: PG-only path; _worker_pool is the canonical pool ref
    await pool.close()

    with pytest.raises(Exception):  # noqa: B017 # Why: connection-loss test — any exception proves the pool is closed; specificity is unnecessary here
        await backend_pair.enqueue(
            EnqueueArgs(
                id=new_job_id(),
                actor="actor_a",
                queue="default",
                payload={},
                max_attempts=3,
                retry_kind="transient",
                scheduled_at=datetime.now(UTC),
            )
        )


# ── Snooze / RetryAfter / reservation equivalence ──


async def test_snooze_cycle_preserves_attempt_round_trip(
    backend_pair: Backend,
) -> None:
    """enqueue → dispatch (attempt=1) → mark_snoozed(delay=30s)
    returns "scheduled" → attempt unchanged at 1 (snooze does not touch
    attempt) → scheduled_to_pending → dispatch increments to attempt=2.
    """
    job_id, wid = await _enqueue_dispatch_any(backend_pair)

    row = await backend_pair.get(job_id)
    assert row is not None
    assert row.attempt == 1

    result = await backend_pair.mark_snoozed(job_id, wid, timedelta(seconds=30))
    assert result == "scheduled"

    # snooze leaves attempt untouched
    row = await backend_pair.get(job_id)
    assert row is not None
    assert row.attempt == 1

    # Advance/promote: for PG, mark_snoozed sets scheduled_at = now() + 30s
    # (server-side); force scheduled_at to the past so scheduled_to_pending
    # can promote. For InMemory, advance the fake clock.
    await _advance_and_promote(backend_pair, _now_for(backend_pair) + timedelta(seconds=31))

    dispatched = await backend_pair.dispatch_batch(
        worker_id=wid,
        queues=["default"],
        limit=1,
        lock_lease=_LOCK_LEASE,
    )
    assert len(dispatched) == 1
    # dispatch increments: 1 → 2
    assert dispatched[0].attempt == 2

    attempts = await backend_pair.get_attempts(job_id)
    assert len(attempts) == 1
    assert attempts[0].outcome == "snoozed"

    events = await _get_events(backend_pair, job_id)
    _assert_state_change_event(events, "running", "scheduled")


async def test_snooze_past_deadline_transitions_to_failed(
    backend_pair: Backend,
) -> None:
    """enqueue with schedule_to_close → dispatch →
    mark_snoozed with 30s delay returns "failed" → row in failed,
    error_class='DeadlineExceeded',
    error_message='schedule_to_close reached before next dispatch',
    attempt row with outcome='failed'.
    """
    # Set schedule_to_close slightly ahead of now so that a 30s delay
    # exceeds it for both backends.
    deadline = _now_for(backend_pair) + timedelta(seconds=5)
    job_id, wid = await _enqueue_dispatch_any(backend_pair)
    await _force_job_state(backend_pair, job_id, schedule_to_close=deadline)

    result = await backend_pair.mark_snoozed(job_id, wid, timedelta(seconds=30))
    assert result == "failed"

    row = await backend_pair.get(job_id)
    assert row is not None
    _assert_job_row(
        row,
        status="failed",
        error_class="DeadlineExceeded",
        error_message="schedule_to_close reached before next dispatch",
        last_heartbeat_at_none=True,
    )

    attempts = await backend_pair.get_attempts(job_id)
    assert len(attempts) == 1
    _assert_attempt_row(
        attempts,
        0,
        outcome="failed",
        error_class="DeadlineExceeded",
        error_message="schedule_to_close reached before next dispatch",
    )

    events = await _get_events(backend_pair, job_id)
    _assert_state_change_event(events, "running", "failed", error_class="DeadlineExceeded")


async def test_retry_after_consume_true_increments_attempt(
    backend_pair: Backend,
) -> None:
    """enqueue → dispatch (attempt=1) →
    mark_retry_after(consume_budget=True) returns "scheduled" → row
    attempt=1 (dispatch increments, not retry_after), status=scheduled,
    attempt row outcome='snoozed', error_class='RetryAfter'.
    """
    job_id, wid = await _enqueue_dispatch_any(backend_pair)

    row = await backend_pair.get(job_id)
    assert row is not None
    assert row.attempt == 1

    result = await backend_pair.mark_retry_after(
        job_id, wid, timedelta(seconds=10), consume_budget=True
    )
    assert result == "scheduled"

    row = await backend_pair.get(job_id)
    assert row is not None
    _assert_job_row(row, status="scheduled", attempt=1, last_heartbeat_at_none=True)

    attempts = await backend_pair.get_attempts(job_id)
    assert len(attempts) == 1
    _assert_attempt_row(attempts, 0, outcome="snoozed", error_class="RetryAfter")

    events = await _get_events(backend_pair, job_id)
    _assert_state_change_event(events, "running", "scheduled")


async def test_retry_after_consume_false_preserves_attempt(
    backend_pair: Backend,
) -> None:
    """same setup, consume_budget=False returns "scheduled" → row
    attempt=1 unchanged.
    """
    job_id, wid = await _enqueue_dispatch_any(backend_pair)

    row = await backend_pair.get(job_id)
    assert row is not None
    assert row.attempt == 1

    result = await backend_pair.mark_retry_after(
        job_id, wid, timedelta(seconds=10), consume_budget=False
    )
    assert result == "scheduled"

    row = await backend_pair.get(job_id)
    assert row is not None
    _assert_job_row(row, status="scheduled", attempt=1, last_heartbeat_at_none=True)

    attempts = await backend_pair.get_attempts(job_id)
    assert len(attempts) == 1
    _assert_attempt_row(attempts, 0, outcome="snoozed", error_class="RetryAfter")

    events = await _get_events(backend_pair, job_id)
    _assert_state_change_event(events, "running", "scheduled")


async def test_retry_after_exhausts_budget_transitions_to_failed(
    backend_pair: Backend,
) -> None:
    """max_attempts=3, retry_kind='transient', dispatch sets
    attempt=3, then mark_retry_after(consume_budget=True) returns
    "failed" → row in failed, error_class='MaxAttemptsExceeded'.
    """
    job_id, wid = await _enqueue_dispatch_any(backend_pair, max_attempts=3, retry_kind="transient")

    # Force attempt=3 to simulate third dispatch without running two prior retries
    await _force_job_state(backend_pair, job_id, attempt=3)

    result = await backend_pair.mark_retry_after(
        job_id, wid, timedelta(seconds=10), consume_budget=True
    )
    assert result == "failed:MaxAttemptsExceeded"

    row = await backend_pair.get(job_id)
    assert row is not None
    _assert_job_row(
        row,
        status="failed",
        error_class="MaxAttemptsExceeded",
        last_heartbeat_at_none=True,
    )

    attempts = await backend_pair.get_attempts(job_id)
    assert len(attempts) == 1
    _assert_attempt_row(attempts, 0, outcome="failed", error_class="MaxAttemptsExceeded")

    events = await _get_events(backend_pair, job_id)
    _assert_state_change_event(events, "running", "failed", error_class="MaxAttemptsExceeded")


async def test_retry_after_indefinite_tier_ignores_max_attempts(
    backend_pair: Backend,
) -> None:
    """retry_kind='indefinite', attempt >= max_attempts → "scheduled",
    not "failed". Indefinite-tier jobs never exhaust their retry budget
    regardless of max_attempts. attempt is not incremented by
    mark_retry_after (dispatch increments attempt).
    """
    job_id, wid = await _enqueue_dispatch_any(backend_pair, max_attempts=3, retry_kind="indefinite")

    await _force_job_state(backend_pair, job_id, attempt=5)

    result = await backend_pair.mark_retry_after(
        job_id, wid, timedelta(seconds=10), consume_budget=True
    )
    assert result == "scheduled"

    row = await backend_pair.get(job_id)
    assert row is not None
    _assert_job_row(row, status="scheduled", attempt=5, last_heartbeat_at_none=True)

    attempts = await backend_pair.get_attempts(job_id)
    assert len(attempts) == 1
    _assert_attempt_row(attempts, 0, outcome="snoozed", error_class="RetryAfter")

    events = await _get_events(backend_pair, job_id)
    _assert_state_change_event(events, "running", "scheduled")


async def test_reservation_unavailable_produces_metadata_annotated_snooze(
    backend_pair: Backend,
) -> None:
    """mark_snoozed with
    metadata_update={"awaiting": "reservation:gpu_pool"},
    outcome="reservation_denied" returns "scheduled" → row's
    metadata['awaiting'] == 'reservation:gpu_pool', attempt row with
    outcome='reservation_denied'.
    """
    job_id, wid = await _enqueue_dispatch_any(backend_pair)

    result = await backend_pair.mark_snoozed(
        job_id,
        wid,
        timedelta(seconds=30),
        metadata_update={"awaiting": "reservation:gpu_pool"},
        outcome="reservation_denied",
    )
    assert result == "scheduled"

    row = await backend_pair.get(job_id)
    assert row is not None
    assert row.metadata.get("awaiting") == "reservation:gpu_pool"

    attempts = await backend_pair.get_attempts(job_id)
    assert len(attempts) == 1
    _assert_attempt_row(attempts, 0, outcome="reservation_denied")

    events = await _get_events(backend_pair, job_id)
    _assert_state_change_event(events, "running", "scheduled")


async def test_mark_snoozed_idempotent_returns_noop(backend_pair: Backend) -> None:
    """Call mark_snoozed twice on the same (job_id, worker_id); second call
    returns "noop" without writing a second attempt row. Both backends
    agree.
    """
    job_id, wid = await _enqueue_dispatch_any(backend_pair)

    result1 = await backend_pair.mark_snoozed(job_id, wid, timedelta(seconds=30))
    assert result1 == "scheduled"

    result2 = await backend_pair.mark_snoozed(job_id, wid, timedelta(seconds=30))
    assert result2 == "noop"

    attempts = await backend_pair.get_attempts(job_id)
    assert len(attempts) == 1


async def test_mark_retry_after_idempotent_returns_noop(
    backend_pair: Backend,
) -> None:
    """Same pattern for mark_retry_after. Both backends agree."""
    job_id, wid = await _enqueue_dispatch_any(backend_pair)

    result1 = await backend_pair.mark_retry_after(
        job_id, wid, timedelta(seconds=10), consume_budget=True
    )
    assert result1 == "scheduled"

    result2 = await backend_pair.mark_retry_after(
        job_id, wid, timedelta(seconds=10), consume_budget=True
    )
    assert result2 == "noop"

    attempts = await backend_pair.get_attempts(job_id)
    assert len(attempts) == 1


# ── PG-compatible terminal-write equivalence tests ──────────────────────
#
# The helpers below bypass dispatch_batch (not yet implemented for PG) and
# create a running job directly, mirroring the chaos-test pattern. The
# tests exercise mark_succeeded, mark_failed_or_retry (terminal-fail
# branch), and mark_cancelled against both backends.


async def _pg_enqueue_dispatch(
    backend: Backend,
    *,
    actor: str = "actor_a",
    queue: str = "default",
    max_attempts: int = 3,
    retry_kind: str = "transient",
) -> tuple[JobId, UUID]:
    """Create a running job on a PostgresBackend via direct SQL.

    Uses private attributes ``_worker_pool`` and ``_schema_name`` under
    ``type: ignore`` — same pattern as the chaos-test helpers. Returns
    (job_id, worker_id).
    """
    import asyncpg as _asyncpg

    worker_pool: _asyncpg.Pool = backend._worker_pool  # type: ignore[reportPrivateUsage] # Why: PG-path test helper; _worker_pool mirrors chaos-test pattern
    schema: str = backend._schema_name  # type: ignore[reportPrivateUsage] # Why: same

    worker_id = new_uuid()
    job_id = new_job_id()

    async with worker_pool.acquire() as conn:  # type: ignore[reportUnknownVariableType] # Why: asyncpg stubs yield PoolConnectionProxy | Unknown; runtime type is correct
        await conn.execute(
            f'INSERT INTO "{schema}".workers (id, hostname, pid, queues) VALUES ($1, $2, $3, $4)',
            worker_id,
            "test-host",
            12345,
            [queue],
        )
        await conn.execute(
            f"""INSERT INTO \"{schema}\".jobs (
                id, actor, queue, payload, max_attempts, retry_kind,
                status, priority, attempt, scheduled_at,
                locked_by_worker, lock_expires_at, started_at, last_heartbeat_at
            ) VALUES (
                $1, $2, $3, $4::jsonb, $5, $6,
                'running', 0, 1, now(),
                $7, now() + interval '60 seconds', now(), now()
            )""",
            job_id,
            actor,
            queue,
            '{"key": "value"}',
            max_attempts,
            retry_kind,
            worker_id,
        )

    return job_id, worker_id


async def _enqueue_dispatch_any(
    backend: Backend,
    *,
    actor: str = "actor_a",
    queue: str = "default",
    max_attempts: int = 3,
    retry_kind: str = "transient",
) -> tuple[JobId, UUID]:
    """Backend-agnostic helper: InMemory uses the standard enqueue+dispatch
    path; PG uses direct SQL insertion (dispatch_batch not yet implemented).
    """
    if isinstance(backend, InMemoryBackend):
        return await _enqueue_dispatch(
            backend,
            actor=actor,
            queue=queue,
            max_attempts=max_attempts,
            retry_kind=retry_kind,
        )
    return await _pg_enqueue_dispatch(
        backend,
        actor=actor,
        queue=queue,
        max_attempts=max_attempts,
        retry_kind=retry_kind,
    )


async def test_mark_succeeded_transitions_row_and_emits_attempt(
    backend_pair: Backend,
) -> None:
    """mark_succeeded: running → succeeded; attempt row outcome='succeeded';
    locked_by_worker=None; last_heartbeat_at unchanged (not cleared by PG SQL).
    Both backends must agree on status, locked state, and attempt count.
    """
    job_id, wid = await _enqueue_dispatch_any(backend_pair)

    result = await backend_pair.mark_succeeded(job_id, wid, {"ok": True})
    assert result is True

    row = await backend_pair.get(job_id)
    assert row is not None
    # locked=True: mark_succeeded does not clear lock fields (PG SQL omits them)
    _assert_job_row(row, status="succeeded", locked=True)

    attempts = await backend_pair.get_attempts(job_id)
    assert len(attempts) == 1
    _assert_attempt_row(attempts, 0, outcome="succeeded")

    # second call is idempotent — returns False, no second attempt row
    result2 = await backend_pair.mark_succeeded(job_id, wid, {"ok": True})
    assert result2 is False
    attempts2 = await backend_pair.get_attempts(job_id)
    assert len(attempts2) == 1


async def test_mark_failed_or_retry_terminal_branch(
    backend_pair: Backend,
) -> None:
    """mark_failed_or_retry(next_scheduled_at=None): running → failed;
    attempt row outcome='failed'; row carries error fields.
    Both backends must agree.
    """
    job_id, wid = await _enqueue_dispatch_any(backend_pair)

    error_info = ErrorInfo(
        error_class="ValueError",
        error_message="something went wrong",
        error_traceback=None,
    )

    row = await backend_pair.mark_failed_or_retry(job_id, wid, error_info, next_scheduled_at=None)
    assert row.status == "failed"
    # locked=True: mark_failed_or_retry does not clear lock fields (PG SQL omits them)
    _assert_job_row(
        row,
        status="failed",
        error_class="ValueError",
        error_message="something went wrong",
        locked=True,
    )

    attempts = await backend_pair.get_attempts(job_id)
    assert len(attempts) == 1
    _assert_attempt_row(
        attempts,
        0,
        outcome="failed",
        error_class="ValueError",
        error_message="something went wrong",
    )


async def test_mark_cancelled_transitions_row_and_emits_attempt(
    backend_pair: Backend,
) -> None:
    """mark_cancelled: running → cancelled; attempt row outcome='cancelled'.
    Both backends must agree. Note: neither backend clears locked_by_worker
    on cancellation — it is preserved as a forensic marker.
    """
    job_id, wid = await _enqueue_dispatch_any(backend_pair)

    result = await backend_pair.mark_cancelled(job_id, wid)
    assert result is True

    row = await backend_pair.get(job_id)
    assert row is not None
    # locked=True: mark_cancelled does not clear lock fields (PG SQL omits them)
    _assert_job_row(row, status="cancelled", locked=True)

    attempts = await backend_pair.get_attempts(job_id)
    assert len(attempts) == 1
    _assert_attempt_row(attempts, 0, outcome="cancelled")

    # second call is idempotent — returns False, no second attempt row
    result2 = await backend_pair.mark_cancelled(job_id, wid)
    assert result2 is False
    attempts2 = await backend_pair.get_attempts(job_id)
    assert len(attempts2) == 1


# ── result_size_bytes equivalence ─────────────────────────────────


async def test_result_size_bytes_set_on_mark_succeeded(backend_pair: Backend) -> None:
    """mark_succeeded with a non-None result sets result_size_bytes
    to a positive integer reflecting the UTF-8 byte length of the serialized
    result. Verifies the A-5 code fix: InMemoryBackend.mark_succeeded now
    sets result_size_bytes (previously it was always None).
    """
    from taskq._json import dumps_str as _json_dumps

    result_payload: dict[str, object] = {"key": "value", "num": 42}
    # Use the same serializer the backend uses so byte counts match exactly.
    expected_size = len(_json_dumps(result_payload).encode("utf-8"))

    job_id, wid = await _enqueue_dispatch_any(backend_pair)
    ok = await backend_pair.mark_succeeded(job_id, wid, result_payload)
    assert ok is True

    row = await backend_pair.get(job_id)
    assert row is not None
    assert row.result_size_bytes is not None, (
        "result_size_bytes must not be None for non-None result"
    )
    assert row.result_size_bytes == expected_size
    assert row.result_size_bytes > 0


async def test_result_size_bytes_none_when_result_is_none(backend_pair: Backend) -> None:
    """mark_succeeded with result=None → result_size_bytes is None."""
    job_id, wid = await _enqueue_dispatch_any(backend_pair)
    await backend_pair.mark_succeeded(job_id, wid, None)

    row = await backend_pair.get(job_id)
    assert row is not None
    assert row.result_size_bytes is None


# ── ownership mismatch raises WorkerOwnershipMismatch ─────────────


async def test_mark_failed_or_retry_ownership_mismatch_raises(backend_pair: Backend) -> None:
    """mark_failed_or_retry called with wrong worker_id raises
    WorkerOwnershipMismatch in both backends.
    """
    from taskq.exceptions import WorkerOwnershipMismatch

    job_id, correct_wid = await _enqueue_dispatch_any(backend_pair)
    wrong_wid = new_uuid()

    error_info = ErrorInfo(
        error_class="RuntimeError",
        error_message="test failure",
        error_traceback=None,
    )

    import pytest

    with pytest.raises(WorkerOwnershipMismatch) as exc_info:
        await backend_pair.mark_failed_or_retry(
            job_id, wrong_wid, error_info, next_scheduled_at=None
        )
    exc = exc_info.value
    assert exc.job_id == job_id
    assert exc.expected == wrong_wid
    assert exc.actual == correct_wid


async def test_mark_succeeded_wrong_worker_returns_false(backend_pair: Backend) -> None:
    """mark_succeeded with wrong worker_id returns False (no mismatch raise)."""
    job_id, _ = await _enqueue_dispatch_any(backend_pair)
    wrong_wid = new_uuid()

    result = await backend_pair.mark_succeeded(job_id, wrong_wid, None)
    # InMemoryBackend returns False for wrong worker (not an error path)
    assert result is False


# ── error_class consistency for crashed jobs ─────────────────────


async def test_reclaim_expired_locks_sets_worker_crashed_error_class(
    backend_pair: Backend,
) -> None:
    """after reclaim_expired_locks, jobs that can no longer retry
    have error_class == None on the jobs row and 'WorkerCrashed' on the
    AttemptRow (fix #4 — PG Sweep 1 SQL does not set error_class on jobs).
    """
    # Enqueue a job that has exhausted its retry budget (max_attempts=1)
    job_id, _wid = await _enqueue_dispatch_any(backend_pair, max_attempts=1)

    # Manually expire the lock by forcing lock_expires_at to the past
    expired_at = _now_for(backend_pair) - timedelta(seconds=1)
    await _force_job_state(backend_pair, job_id, lock_expires_at=expired_at)

    # Reclaim expired locks
    reclaimed = await backend_pair.reclaim_expired_locks(
        _now_for(backend_pair),
        cancel_grace=timedelta(seconds=30),
        cleanup_grace=timedelta(seconds=10),
    )
    assert reclaimed == 1

    row_after = await backend_pair.get(job_id)
    assert row_after is not None
    assert row_after.status == "crashed"
    assert row_after.error_class is None, (
        f"expected None (error_class lives on AttemptRow), got {row_after.error_class!r}"
    )

    attempts = await backend_pair.get_attempts(job_id)
    assert len(attempts) == 1
    assert attempts[0].error_class == "WorkerCrashed"
    assert attempts[0].outcome == "crashed"


# ── InMemoryBackend.reclaim_expired_locks writes job_events row ──


async def test_reclaim_expired_locks_writes_job_events_row(
    backend_pair: Backend,
) -> None:
    """reclaim_expired_locks writes a job_events row with
    kind='state_change' and reason='lock_expired' for crashed jobs, matching
    the PostgresBackend.sweep_expired_locks pattern.
    """
    # max_attempts=1 → no retry available → crashes
    job_id, _wid = await _enqueue_dispatch_any(backend_pair, max_attempts=1)

    # Force lock_expires_at to the past
    expired_at = _now_for(backend_pair) - timedelta(seconds=1)
    await _force_job_state(backend_pair, job_id, lock_expires_at=expired_at)

    await backend_pair.reclaim_expired_locks(
        _now_for(backend_pair),
        cancel_grace=timedelta(seconds=30),
        cleanup_grace=timedelta(seconds=10),
    )

    events = await _get_events(backend_pair, job_id)
    lock_expired_events = [
        e for e in events if e.kind == "state_change" and e.detail.get("reason") == "lock_expired"
    ]
    assert len(lock_expired_events) >= 1, (
        f"expected at least one state_change event with reason='lock_expired', got events: {events}"
    )
    evt = lock_expired_events[0]
    assert evt.detail.get("to_state") == "crashed"
    assert evt.detail.get("from_state") == "running"


async def test_reclaim_expired_locks_retryable_job_writes_events_row(
    backend_pair: Backend,
) -> None:
    """reclaim_expired_locks for a retryable job
    writes a job_events state_change running → pending with reason='lock_expired'.
    """
    # max_attempts=3 → has remaining budget → retries
    job_id, _wid = await _enqueue_dispatch_any(backend_pair, max_attempts=3)

    # Force lock_expires_at to the past
    expired_at = _now_for(backend_pair) - timedelta(seconds=1)
    await _force_job_state(backend_pair, job_id, lock_expires_at=expired_at)

    await backend_pair.reclaim_expired_locks(
        _now_for(backend_pair),
        cancel_grace=timedelta(seconds=30),
        cleanup_grace=timedelta(seconds=10),
    )

    events = await _get_events(backend_pair, job_id)
    lock_expired_events = [
        e for e in events if e.kind == "state_change" and e.detail.get("reason") == "lock_expired"
    ]
    assert len(lock_expired_events) >= 1, (
        f"expected state_change event with reason='lock_expired' for retry path, got: {events}"
    )
    evt = lock_expired_events[0]
    assert evt.detail.get("from_state") == "running"
    assert evt.detail.get("to_state") == "pending"


# ── mark_snoozed deadline boundary (delay == remaining_budget) ──


async def test_mark_snoozed_delay_exactly_equal_remaining_budget_fails(
    backend_pair: Backend,
) -> None:
    """mark_snoozed where new_scheduled_at == schedule_to_close
    returns 'scheduled' (boundary: the > check rejects when new_scheduled >
    deadline, and == does NOT cross the > boundary).

    For InMemory: advance clock to deadline so clock.now() == schedule_to_close.
    For PG: schedule_to_close is in the future relative to server-side now(),
    so now() + delay(0s) <= schedule_to_close → snooze arm fires.
    """
    # Set schedule_to_close in the future relative to the backend's "now"
    deadline = _now_for(backend_pair) + timedelta(hours=12)
    job_id, wid = await _enqueue_dispatch_any(backend_pair)
    await _force_job_state(backend_pair, job_id, schedule_to_close=deadline)

    # For InMemory: advance clock to the deadline so now == schedule_to_close.
    # For PG: no clock manipulation needed — server-side now() < schedule_to_close.
    if isinstance(backend_pair, InMemoryBackend):
        backend_pair.advance_clock_to(deadline)

    result = await backend_pair.mark_snoozed(job_id, wid, timedelta(seconds=0))
    # == boundary: new_scheduled_at == schedule_to_close → guard is >, so NOT rejected
    assert result == "scheduled"


async def test_mark_snoozed_delay_exceeds_deadline_fails(
    backend_pair: Backend,
) -> None:
    """mark_snoozed where new_scheduled_at > schedule_to_close
    returns 'failed'. This is the strict > check.
    """
    # Deadline is 5s from now; delay is 30s → would exceed deadline
    deadline = _now_for(backend_pair) + timedelta(seconds=5)
    job_id, wid = await _enqueue_dispatch_any(backend_pair)
    await _force_job_state(backend_pair, job_id, schedule_to_close=deadline)

    result = await backend_pair.mark_snoozed(job_id, wid, timedelta(seconds=30))
    assert result == "failed"


# ── write_cancel_request on terminal job returns False ────────────


async def test_write_cancel_request_on_succeeded_job_returns_false(
    backend_pair: Backend,
) -> None:
    """write_cancel_request on a terminal (succeeded) job returns
    False (idempotent no-op).
    """
    job_id, wid = await _enqueue_dispatch_any(backend_pair)
    await backend_pair.mark_succeeded(job_id, wid, None)

    result = await backend_pair.write_cancel_request(job_id, reason=None)
    assert result is False


async def test_write_cancel_request_on_failed_job_returns_false(
    backend_pair: Backend,
) -> None:
    """write_cancel_request on a terminal (failed) job returns False."""
    job_id, wid = await _enqueue_dispatch_any(backend_pair)
    error_info = ErrorInfo(
        error_class="RuntimeError",
        error_message="fail",
        error_traceback=None,
    )
    await backend_pair.mark_failed_or_retry(job_id, wid, error_info, next_scheduled_at=None)

    result = await backend_pair.write_cancel_request(job_id, reason=None)
    assert result is False


# ── Dispatch equivalence ───────────────────────────────────────
#
# Parametric tests via backend_pair asserting both backends produce the same
# observable dispatch outcomes for non-concurrent scenarios. Only counts
# and field invariants are asserted — no per-row identity assertions across
# backends (determinism note: PG index scan order is not guaranteed to match
# in-memory sort order when candidates share identical (priority, scheduled_at)).


async def _dispatch_worker_id(backend: Backend) -> UUID:
    """Return a worker_id suitable for dispatch_batch on either backend."""
    if isinstance(backend, InMemoryBackend):
        return backend._worker_id  # type: ignore[reportPrivateUsage] # Why: _worker_id is the canonical worker identity for InMemoryBackend dispatch
    return new_uuid()


def _assert_dispatched_rows(
    rows: list[JobRow],
    *,
    count: int,
    worker_id: UUID,
    status: str = "running",
    attempt: int = 1,
) -> None:
    """Assert count and field invariants on dispatched rows."""
    assert len(rows) == count
    for r in rows:
        assert r.status == status
        assert r.attempt == attempt
        assert r.locked_by_worker == worker_id
        assert r.lock_expires_at is not None


# ── single-actor cap ──────────────────────────────────────────────


async def test_eq1_single_actor_cap(backend_pair: Backend) -> None:
    """register actor with max_concurrent=2, enqueue 5 jobs,
    dispatch with limit=10 → exactly 2 running on both backends."""
    await _set_actor_cap(backend_pair, actor="actor_a", max_concurrent=2)

    for _ in range(5):
        await backend_pair.enqueue(
            EnqueueArgs(
                id=new_job_id(),
                actor="actor_a",
                queue="default",
                payload={},
                max_attempts=3,
                retry_kind="transient",
                scheduled_at=_START,
            )
        )

    wid = await _dispatch_worker_id(backend_pair)
    dispatched = await backend_pair.dispatch_batch(
        worker_id=wid, queues=["default"], limit=10, lock_lease=_LOCK_LEASE
    )
    _assert_dispatched_rows(dispatched, count=2, worker_id=wid)

    all_jobs = await backend_pair.list_jobs(JobFilter(limit=100))
    pending = [r for r in all_jobs if r.status == "pending"]
    running = [r for r in all_jobs if r.status == "running"]
    assert len(running) == 2
    assert len(pending) == 3


# ── identity serialization ────────────────────────────────────────


async def test_eq2_identity_serialization(backend_pair: Backend) -> None:
    """enqueue 3 jobs with same (actor, identity_key), max_concurrent=10
    → exactly 1 running on both backends."""
    await _set_actor_cap(backend_pair, actor="actor_a", max_concurrent=10)

    shared_ikey = IdentityKey("idem-key-1")
    for _ in range(3):
        await backend_pair.enqueue(
            EnqueueArgs(
                id=new_job_id(),
                actor="actor_a",
                queue="default",
                payload={},
                max_attempts=3,
                retry_kind="transient",
                scheduled_at=_START,
                identity_key=shared_ikey,
            )
        )

    wid = await _dispatch_worker_id(backend_pair)
    dispatched = await backend_pair.dispatch_batch(
        worker_id=wid, queues=["default"], limit=10, lock_lease=_LOCK_LEASE
    )
    _assert_dispatched_rows(dispatched, count=1, worker_id=wid)
    assert dispatched[0].identity_key == shared_ikey

    all_jobs = await backend_pair.list_jobs(JobFilter(limit=100))
    running = [r for r in all_jobs if r.status == "running"]
    pending = [r for r in all_jobs if r.status == "pending"]
    assert len(running) == 1
    assert len(pending) == 2


# ── multi-identity, same actor ────────────────────────────────────


async def test_eq3_multi_identity_same_actor(backend_pair: Backend) -> None:
    """enqueue 3 jobs with different identity_keys, max_concurrent=10
    → all 3 dispatch on both backends."""
    await _set_actor_cap(backend_pair, actor="actor_a", max_concurrent=10)

    ikeys = [IdentityKey(f"idem-{i}") for i in range(3)]
    for ikey in ikeys:
        await backend_pair.enqueue(
            EnqueueArgs(
                id=new_job_id(),
                actor="actor_a",
                queue="default",
                payload={},
                max_attempts=3,
                retry_kind="transient",
                scheduled_at=_START,
                identity_key=ikey,
            )
        )

    wid = await _dispatch_worker_id(backend_pair)
    dispatched = await backend_pair.dispatch_batch(
        worker_id=wid, queues=["default"], limit=10, lock_lease=_LOCK_LEASE
    )
    _assert_dispatched_rows(dispatched, count=3, worker_id=wid)

    dispatched_ikeys = {r.identity_key for r in dispatched}
    assert len(dispatched_ikeys) == 3


# ── unbounded actor ───────────────────────────────────────────────


async def test_eq4_unbounded_actor(backend_pair: Backend) -> None:
    """register max_concurrent=None, enqueue 50 jobs, dispatch limit=20
    → 20 dispatched on both backends."""
    await _set_actor_cap(backend_pair, actor="actor_a", max_concurrent=None)

    for _ in range(50):
        await backend_pair.enqueue(
            EnqueueArgs(
                id=new_job_id(),
                actor="actor_a",
                queue="default",
                payload={},
                max_attempts=3,
                retry_kind="transient",
                scheduled_at=_START,
            )
        )

    wid = await _dispatch_worker_id(backend_pair)
    dispatched = await backend_pair.dispatch_batch(
        worker_id=wid, queues=["default"], limit=20, lock_lease=_LOCK_LEASE
    )
    _assert_dispatched_rows(dispatched, count=20, worker_id=wid)


# ── empty queues ──────────────────────────────────────────────────


async def test_eq5_empty_queues(backend_pair: Backend) -> None:
    """dispatch with queues=["nonexistent"] → empty result on both
    backends."""
    wid = await _dispatch_worker_id(backend_pair)
    dispatched = await backend_pair.dispatch_batch(
        worker_id=wid,
        queues=["nonexistent"],
        limit=10,
        lock_lease=_LOCK_LEASE,
    )
    assert dispatched == []


# ── schedule_to_close filter ──────────────────────────────────────


async def test_eq6_schedule_to_close_filter(backend_pair: Backend) -> None:
    """enqueue one job with schedule_to_close in the past, another with
    no deadline → only the un-deadlined job dispatches on both backends."""
    await _set_actor_cap(backend_pair, actor="actor_a", max_concurrent=10)

    deadlined_id = new_job_id()
    await backend_pair.enqueue(
        EnqueueArgs(
            id=deadlined_id,
            actor="actor_a",
            queue="default",
            payload={},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=_START,
            schedule_to_close=_START - timedelta(hours=10),
        )
    )

    open_id = new_job_id()
    await backend_pair.enqueue(
        EnqueueArgs(
            id=open_id,
            actor="actor_a",
            queue="default",
            payload={},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=_START,
        )
    )

    wid = await _dispatch_worker_id(backend_pair)
    dispatched = await backend_pair.dispatch_batch(
        worker_id=wid, queues=["default"], limit=10, lock_lease=_LOCK_LEASE
    )
    _assert_dispatched_rows(dispatched, count=1, worker_id=wid)
    assert dispatched[0].id == open_id
    assert dispatched[0].schedule_to_close is None


# ── per-actor row-count cap (actor_rank) ──────────────────────────


async def test_eq7_per_actor_row_count_cap(backend_pair: Backend) -> None:
    """register actor A with max_concurrent=2, enqueue 10 jobs for A
    (no other actors), dispatch limit=20 → exactly 2 dispatch. The
    actor_rank window function caps the per-batch dispatch count to
    max_concurrent - in_flight even when the boolean gate alone would let
    all 10 pass."""
    await _set_actor_cap(backend_pair, actor="A", max_concurrent=2)

    for _ in range(10):
        await backend_pair.enqueue(
            EnqueueArgs(
                id=new_job_id(),
                actor="A",
                queue="default",
                payload={},
                max_attempts=3,
                retry_kind="transient",
                scheduled_at=_START,
            )
        )

    wid = await _dispatch_worker_id(backend_pair)
    dispatched = await backend_pair.dispatch_batch(
        worker_id=wid, queues=["default"], limit=20, lock_lease=_LOCK_LEASE
    )
    _assert_dispatched_rows(dispatched, count=2, worker_id=wid)


# ── Indefinite-tier equivalence: enqueue → fail → re-dispatch → succeed ──


async def test_indefinite_tier_fail_retry_succeed(backend_pair: Backend) -> None:
    """Indefinite-tier cross-backend equivalence: enqueue a job with
    schedule_to_close in the future, dispatch, fail via mark_failed_or_retry
    with next_scheduled_at, re-dispatch, and succeed. Validates both
    backends produce consistent row state across the indefinite retry cycle."""
    from taskq.backend._protocol import ErrorInfo as _ErrorInfo
    from taskq.retry import RetryPolicy as _RetryPolicy

    # Set schedule_to_close in the future so the job doesn't deadline
    deadline = _now_for(backend_pair) + timedelta(hours=2)
    job_id, wid = await _enqueue_dispatch_any(
        backend_pair,
        retry_kind="indefinite",
    )
    await _force_job_state(backend_pair, job_id, schedule_to_close=deadline)

    row = await backend_pair.get(job_id)
    assert row is not None
    assert row.attempt == 1
    assert row.status == "running"

    error_info = _ErrorInfo(
        error_class="RuntimeError",
        error_message="temporary failure",
        error_traceback=None,
    )
    policy = _RetryPolicy(kind="indefinite", time_budget=timedelta(hours=2), jitter=0.0)
    from taskq.retry import compute_backoff as _compute_backoff

    next_scheduled = _now_for(backend_pair) + timedelta(seconds=1) + _compute_backoff(policy, 1)
    row_after = await backend_pair.mark_failed_or_retry(job_id, wid, error_info, next_scheduled)
    assert row_after.status == "scheduled"
    assert row_after.attempt == 1

    # Advance/promote: force scheduled_at to the past for PG, advance fake
    # clock for InMemory, then call scheduled_to_pending.
    await _advance_and_promote(backend_pair, next_scheduled + timedelta(seconds=1))
    dispatched = await backend_pair.dispatch_batch(
        worker_id=wid,
        queues=["default"],
        limit=1,
        lock_lease=_LOCK_LEASE,
    )
    assert len(dispatched) == 1
    assert dispatched[0].attempt == 2
    assert dispatched[0].status == "running"

    await backend_pair.mark_succeeded(job_id, wid, {"ok": True})
    row = await backend_pair.get(job_id)
    assert row is not None
    assert row.status == "succeeded"
    assert row.attempt == 2

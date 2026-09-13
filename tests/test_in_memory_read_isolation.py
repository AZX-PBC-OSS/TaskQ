"""InMemoryBackend must not alias stored state at any seam.

PostgresBackend materialises a fresh ``JobRow`` from the SQL record on every
read, so no caller-held object can ever reach stored state. InMemoryBackend
must honour the same contract — otherwise a test that mutates a row it
obtained (or a dict it handed in) silently corrupts the backend, and code
that is correct against PG fails only under the real engine. ``JobRow`` is
frozen, but its ``payload`` / ``progress_state`` / ``result`` / ``metadata``
fields are plain dicts shared by reference.

``get`` already isolates (``_read_copy``). These tests pin the same
isolation guarantee at every other read seam and at every write seam that
accepts a caller-owned dict: mutating the object a caller holds must never
change what a later ``get`` (or ``get_archived``) returns.
"""

from datetime import UTC, datetime, timedelta

from taskq._ids import new_job_id, new_uuid
from taskq.backend._protocol import (
    BatchFilter,
    EnqueueArgs,
    ErrorInfo,
    IdentityKey,
    JobFilter,
    JobId,
    ScheduleCreateArgs,
    ScheduleUpdateArgs,
)
from taskq.testing._runner import archive_terminal_jobs
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend

_START = datetime(2025, 1, 1, tzinfo=UTC)


def _make_backend() -> InMemoryBackend:
    return InMemoryBackend(clock=FakeClock(_START))


def _args(
    *,
    payload: dict[str, object] | None = None,
    metadata: dict[str, object] | None = None,
    actor: str = "test_actor",
    identity_key: str | None = None,
    unique_for: timedelta | None = None,
) -> EnqueueArgs:
    return EnqueueArgs(
        id=new_job_id(),
        actor=actor,
        queue="default",
        payload=payload if payload is not None else {"key": "value"},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=_START,
        identity_key=IdentityKey(identity_key) if identity_key is not None else None,
        unique_for=unique_for,
        metadata=metadata if metadata is not None else {},
    )


async def _stored_payload(backend: InMemoryBackend, job_id: JobId) -> dict[str, object]:
    """Read the stored payload back through ``get`` — the one seam already
    guaranteed to isolate, so it reflects storage rather than the alias."""
    fresh = await backend.get(job_id)
    assert fresh is not None
    return fresh.payload


async def test_enqueue_returned_row_does_not_alias_storage() -> None:
    backend = _make_backend()

    returned = await backend.enqueue(_args())
    returned.payload["injected"] = True

    assert "injected" not in await _stored_payload(backend, returned.id)


async def test_enqueue_does_not_store_caller_payload_by_reference() -> None:
    backend = _make_backend()
    payload: dict[str, object] = {"key": "value"}

    row = await backend.enqueue(_args(payload=payload))
    payload["injected"] = True

    assert "injected" not in await _stored_payload(backend, row.id)


async def test_enqueue_does_not_store_caller_metadata_by_reference() -> None:
    backend = _make_backend()
    metadata: dict[str, object] = {"origin": "test"}

    row = await backend.enqueue(_args(metadata=metadata))
    metadata["injected"] = True

    fresh = await backend.get(row.id)
    assert fresh is not None
    assert "injected" not in fresh.metadata


async def test_unique_for_dedup_returned_row_does_not_alias_storage() -> None:
    backend = _make_backend()
    first = await backend.enqueue(
        _args(identity_key="same-identity", unique_for=timedelta(minutes=5))
    )

    deduped = await backend.enqueue(
        _args(identity_key="same-identity", unique_for=timedelta(minutes=5))
    )
    assert deduped.id == first.id  # dedup hit, not a second row
    deduped.payload["injected"] = True

    assert "injected" not in await _stored_payload(backend, first.id)


async def test_list_jobs_rows_do_not_alias_storage() -> None:
    backend = _make_backend()
    row = await backend.enqueue(_args())

    listed = await backend.list_jobs(JobFilter())
    assert len(listed) == 1
    listed[0].payload["injected"] = True

    assert "injected" not in await _stored_payload(backend, row.id)


async def test_dispatch_batch_rows_do_not_alias_storage() -> None:
    backend = _make_backend()
    row = await backend.enqueue(_args())

    claimed = await backend.dispatch_batch(new_uuid(), ["default"], 10, timedelta(seconds=60))
    assert len(claimed) == 1
    claimed[0].payload["injected"] = True

    assert "injected" not in await _stored_payload(backend, row.id)


async def test_mark_succeeded_does_not_store_caller_result_by_reference() -> None:
    backend = _make_backend()
    row = await backend.enqueue(_args())
    worker_id = new_uuid()
    claimed = await backend.dispatch_batch(worker_id, ["default"], 10, timedelta(seconds=60))
    assert len(claimed) == 1

    result: dict[str, object] = {"value": 1}
    await backend.mark_succeeded(row.id, worker_id, result)
    result["injected"] = True

    fresh = await backend.get(row.id)
    assert fresh is not None
    assert fresh.result == {"value": 1}


async def test_get_archived_row_does_not_alias_storage() -> None:
    clock = FakeClock(_START)
    backend = InMemoryBackend(clock=clock)
    row = await backend.enqueue(_args())
    worker_id = new_uuid()
    claimed = await backend.dispatch_batch(worker_id, ["default"], 10, timedelta(seconds=60))
    assert len(claimed) == 1
    await backend.mark_succeeded(row.id, worker_id, {"value": 1})

    clock.advance(timedelta(days=1))
    archive_terminal_jobs(
        backend, retention=timedelta(hours=1), archive_retention=timedelta(days=7)
    )

    archived = await backend.get_archived(row.id)
    assert archived is not None
    archived.row.payload["injected"] = True

    reread = await backend.get_archived(row.id)
    assert reread is not None
    assert "injected" not in reread.row.payload


async def test_get_events_rows_do_not_alias_storage() -> None:
    """The event seams hold the same contract: ``EventRow.detail`` is a
    plain dict on a frozen shell, and PG materialises a fresh row per
    read, so mutating an event read back must never corrupt ``_events``."""
    backend = _make_backend()
    row = await backend.enqueue(_args())
    worker_id = new_uuid()
    claimed = await backend.dispatch_batch(worker_id, ["default"], 10, timedelta(seconds=60))
    assert len(claimed) == 1
    await backend.mark_succeeded(row.id, worker_id, {"value": 1})

    events = await backend.get_events(row.id)
    assert events, "expected at least one state-change event"
    events[0].detail["injected"] = True

    reread = await backend.get_events(row.id)
    assert all("injected" not in e.detail for e in reread)


async def test_get_attempts_rows_do_not_alias_storage() -> None:
    """``AttemptRow.metadata`` is a plain dict on a frozen shell; the
    attempts seam must isolate it like every other read."""
    backend = _make_backend()
    row = await backend.enqueue(_args())
    worker_id = new_uuid()
    claimed = await backend.dispatch_batch(worker_id, ["default"], 10, timedelta(seconds=60))
    assert len(claimed) == 1
    await backend.mark_succeeded(row.id, worker_id, {"value": 1})

    attempts = await backend.get_attempts(row.id)
    assert attempts, "expected the success attempt to be recorded"
    attempts[0].metadata["injected"] = True

    reread = await backend.get_attempts(row.id)
    assert all("injected" not in a.metadata for a in reread)


async def test_write_attempt_does_not_store_caller_row_by_reference() -> None:
    """The write side of the same contract: a caller-held AttemptRow
    stored by reference would let later caller mutation reach storage
    (PG serialises the row at INSERT time)."""
    from taskq.backend._protocol import AttemptRow

    backend = _make_backend()
    row = await backend.enqueue(_args())
    worker_id = new_uuid()
    claimed = await backend.dispatch_batch(worker_id, ["default"], 10, timedelta(seconds=60))
    assert len(claimed) == 1

    attempt = AttemptRow(
        job_id=row.id,
        attempt=1,
        started_at=_START,
        finished_at=_START,
        outcome="succeeded",
        error_class=None,
        error_message=None,
        error_traceback=None,
        duration_ms=1,
        worker_id=worker_id,
        metadata={"origin": "test"},
    )
    await backend.write_attempt(attempt)
    attempt.metadata["injected"] = True

    reread = await backend.get_attempts(row.id)
    assert all("injected" not in a.metadata for a in reread)


async def test_mark_failed_or_retry_returned_row_does_not_alias_storage() -> None:
    """``mark_failed_or_retry`` returns a JobRow — the stored row, not a
    copy, would alias storage exactly like the read seams."""
    backend = _make_backend()
    row = await backend.enqueue(_args())
    worker_id = new_uuid()
    claimed = await backend.dispatch_batch(worker_id, ["default"], 10, timedelta(seconds=60))
    assert len(claimed) == 1

    returned = await backend.mark_failed_or_retry(
        row.id,
        worker_id,
        ErrorInfo(error_class="TestError", error_message="boom", error_traceback=None),
        None,
    )
    returned.payload["injected"] = True

    fresh = await backend.get(row.id)
    assert fresh is not None
    assert "injected" not in fresh.payload


def _schedule_args(metadata: dict[str, object]) -> ScheduleCreateArgs:
    return ScheduleCreateArgs(
        actor="sched_actor",
        cron_expr="*/5 * * * *",
        timezone="UTC",
        next_fire_at=_START,
        metadata=metadata,
    )


async def test_create_schedule_does_not_alias_storage() -> None:
    """Both directions of the create seam: the caller's metadata dict must
    not be stored by reference, and the returned record must not be the
    stored object (PG serialises at INSERT and materialises per read)."""
    backend = _make_backend()
    caller_metadata: dict[str, object] = {"origin": "test"}

    returned = await backend.create_schedule(_schedule_args(caller_metadata))
    caller_metadata["injected"] = True
    returned.metadata["injected"] = True

    listed = await backend.list_schedules(actor="sched_actor")
    assert len(listed) == 1
    assert "injected" not in listed[0].metadata


async def test_list_schedules_rows_do_not_alias_storage() -> None:
    backend = _make_backend()
    await backend.create_schedule(_schedule_args({"origin": "test"}))

    listed = await backend.list_schedules()
    listed[0].metadata["injected"] = True

    reread = await backend.list_schedules()
    assert "injected" not in reread[0].metadata


async def test_update_schedule_does_not_alias_storage() -> None:
    """Both directions of the update seam: caller metadata copied in,
    stored record copied out."""
    backend = _make_backend()
    created = await backend.create_schedule(_schedule_args({"origin": "test"}))
    caller_metadata: dict[str, object] = {"update": True}

    returned = await backend.update_schedule(
        created.id, ScheduleUpdateArgs(metadata=caller_metadata)
    )
    caller_metadata["injected"] = True
    returned.metadata["injected"] = True

    reread = await backend.list_schedules(actor="sched_actor")
    assert "injected" not in reread[0].metadata


async def test_get_batch_row_does_not_alias_storage() -> None:
    backend = _make_backend()
    batch_id = new_uuid()
    await backend.create_batch(
        batch_id,
        "default",
        expected_size=2,
        failure_threshold=None,
        finalizer_job_id=None,
        originating_actor=None,
    )

    fetched = await backend.get_batch(batch_id)
    assert fetched is not None
    fetched.metadata["injected"] = True

    reread = await backend.get_batch(batch_id)
    assert reread is not None
    assert "injected" not in reread.metadata


async def test_list_batches_rows_do_not_alias_storage() -> None:
    backend = _make_backend()
    batch_id = new_uuid()
    await backend.create_batch(
        batch_id,
        "default",
        expected_size=2,
        failure_threshold=None,
        finalizer_job_id=None,
        originating_actor=None,
    )

    listed = await backend.list_batches(BatchFilter())
    assert listed
    listed[0][0].metadata["injected"] = True

    reread = await backend.list_batches(BatchFilter())
    assert "injected" not in reread[0][0].metadata

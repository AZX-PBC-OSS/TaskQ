"""InMemoryBackend seam registry: a NEW public method fails here until its
aliasing contract is classified.

The class this file guards: a seam of ``InMemoryBackend`` that hands out (or
stores) an object aliasing stored state. ``PostgresBackend`` materialises a
fresh row from the SQL record on every read and serialises caller dicts on
every write, so no caller-held object can ever reach stored state; the mirror
must honour the same contract or a test passes over code that would corrupt
on PG. Every seam of the current surface is either pinned by a behavioural
isolation test (named below), probed below, or registered with the reason no
stored state can escape. Those tests guard their seam. This file guards the
*surface*: it walks every public member of ``InMemoryBackend``, so a new
method - the next ``get_events``, the next batch-read seam - fails the
completeness check on arrival, before anyone has to remember the invariant.

Precedent: ``tests/test_sweepaudit_bounded_writes.py`` - dynamic tests pin
each known site, a registry over the walked surface catches the next one.

What to do when the completeness check fails on a method you added:

* If it returns a row type or accepts a caller-held mutable object, add a
  behavioural isolation test (the shape lives in
  ``tests/test_in_memory_read_isolation.py``) and register the pin here -
  or write the probe in this file alongside ``get`` and the batch variants.
* If no stored state can escape (scalar/derived return, no caller-held
  mutable argument), register it in ``_NO_STORED_STATE_ESCAPES`` with the
  reason. The annotation tripwire will check the return half of that claim.
* If it is test wiring whose sharing is the point, register it in
  ``_WIRING_SHARED_BY_DESIGN``. That sentence is the review.
"""

import inspect
from collections.abc import Callable
from datetime import UTC, datetime
from importlib import import_module

from taskq._ids import new_job_id, new_uuid
from taskq.backend._protocol import EnqueueArgs, JobId
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend

_START = datetime(2025, 1, 1, tzinfo=UTC)

#: Return-annotation names that mean "hands out row objects". A method whose
#: annotation names one of these can never sit in _NO_STORED_STATE_ESCAPES.
_ROW_TYPE_NAMES = (
    "JobRow",
    "AttemptRow",
    "EventRow",
    "ScheduleRecord",
    "BatchRow",
    "_ArchivedJobRow",
)

#: Seams pinned by a named behavioural test. Each entry: method name ->
#: (test module, test function, pin mentions the method in its source).
#: ``mention=False`` entries pin the *shared mechanism* the method routes
#: through rather than the method itself; the reason in the adjacent comment
#: names the shared helper.
_PINNED_BY_TEST: dict[str, tuple[str, str, bool]] = {
    "enqueue": (
        "tests.test_in_memory_read_isolation",
        "test_enqueue_returned_row_does_not_alias_storage",
        True,
    ),
    "list_jobs": (
        "tests.test_in_memory_read_isolation",
        "test_list_jobs_rows_do_not_alias_storage",
        True,
    ),
    "dispatch_batch": (
        "tests.test_in_memory_read_isolation",
        "test_dispatch_batch_rows_do_not_alias_storage",
        True,
    ),
    "get_archived": (
        "tests.test_in_memory_read_isolation",
        "test_get_archived_row_does_not_alias_storage",
        True,
    ),
    "get_events": (
        "tests.test_in_memory_read_isolation",
        "test_get_events_rows_do_not_alias_storage",
        True,
    ),
    "poll_reclaim_events": (
        "tests.test_in_memory_read_isolation",
        "test_poll_reclaim_events_rows_do_not_alias_storage",
        True,
    ),
    "get_attempts": (
        "tests.test_in_memory_read_isolation",
        "test_get_attempts_rows_do_not_alias_storage",
        True,
    ),
    "write_attempt": (
        "tests.test_in_memory_read_isolation",
        "test_write_attempt_does_not_store_caller_row_by_reference",
        True,
    ),
    "mark_failed_or_retry": (
        "tests.test_in_memory_read_isolation",
        "test_mark_failed_or_retry_returned_row_does_not_alias_storage",
        True,
    ),
    "create_schedule": (
        "tests.test_in_memory_read_isolation",
        "test_create_schedule_does_not_alias_storage",
        True,
    ),
    "list_schedules": (
        "tests.test_in_memory_read_isolation",
        "test_list_schedules_rows_do_not_alias_storage",
        True,
    ),
    "update_schedule": (
        "tests.test_in_memory_read_isolation",
        "test_update_schedule_does_not_alias_storage",
        True,
    ),
    "get_batch": (
        "tests.test_in_memory_read_isolation",
        "test_get_batch_row_does_not_alias_storage",
        True,
    ),
    "list_batches": (
        "tests.test_in_memory_read_isolation",
        "test_list_batches_rows_do_not_alias_storage",
        True,
    ),
    "mark_succeeded": (
        "tests.test_in_memory_read_isolation",
        "test_mark_succeeded_does_not_store_caller_result_by_reference",
        True,
    ),
    "mark_snoozed": (
        "tests.test_json_non_str_keys",
        "test_in_memory_snooze_metadata_update_stores_pg_jsonb_round_trip_values",
        True,
    ),
    # Shared-mechanism pins: the method routes its caller-held dicts through
    # a helper the named test pins at a sibling consumer.
    "mark_succeeded_with_conn": (  # delegates to _mark_succeeded
        "tests.test_in_memory_read_isolation",
        "test_mark_succeeded_does_not_store_caller_result_by_reference",
        False,
    ),
    "mark_retry_after": (  # progress_state flows through _merge_progress
        "tests.test_terminal_result_serialization",
        "test_in_memory_progress_state_with_nul_raises_value_error",
        False,
    ),
    "mark_interrupted": (  # progress_state flows through _merge_progress; returns str literal
        "tests.test_terminal_result_serialization",
        "test_in_memory_progress_state_with_nul_raises_value_error",
        False,
    ),
}

#: Seams probed behaviourally in this file (below). ``get`` is the read-back
#: seam every isolation test asserts through; the batch/with_conn variants
#: delegate to the pinned ``_enqueue`` builder, and the probes below keep
#: that delegation honest - a re-implementation that stops copying fails a
#: probe here, not nothing.
_PROBED_IN_THIS_FILE = {
    "get",
    "enqueue_with_conn",
    "enqueue_batch",
    "enqueue_batch_fast",
    "enqueue_batch_atomic",
}

#: Methods no stored state can escape through: scalar or freshly-derived
#: return, no caller-held mutable argument. The annotation tripwire below
#: re-checks the return half of each claim - a method edited to return a
#: row type fails there until reclassified.
_NO_STORED_STATE_ESCAPES: dict[str, str] = {
    "abort_batch": "returns a count",
    "archive_terminal_jobs": "returns PruneResult — freshly built counts",
    "cancel_where": "returns BulkCancelResult — frozen, counts only",
    "complete_batch": "returns None; scalar arguments",
    "count_active_jobs": "returns a count",
    "count_batch_non_terminal": "returns a count",
    "count_pending_jobs": "returns a freshly aggregated dict of counts",
    "create_batch": "returns None; builds its own metadata dict, scalar arguments",
    "deadline_sweep": "returns a count",
    "delete_schedule": "returns None",
    "expire_archived_jobs": "returns ArchiveExpiryResult — freshly built counts",
    "extend_reservation_leases": "returns a count",
    "get_actor_max_pending": "returns a freshly built dict of scalars",
    "heartbeat_jobs": "returns a count",
    "increment_batch_failures": "returns a tuple of scalars",
    "mark_abandoned": "returns bool",
    "mark_cancelled": "returns bool",
    "poll_cancel_flags": "returns CancelFlag — frozen, scalar fields only",
    "prune_old_batches": "returns a count",
    "reclaim_expired_locks": "returns a count",
    "reset_batch_failures": "returns a count",
    "retry_job": "returns bool",
    "scheduled_to_pending": "returns a count",
    "set_queue_mode": "returns None; scalar arguments",
    "write_cancel_escalation": "returns bool; scalar arguments",
    "write_cancel_request": "returns bool; builds its own event detail",
}

#: Caller-held data stored by reference, but unreachable from any read seam.
#: If a read seam is ever added for this state, it must copy - and the method
#: moves to a pinned bucket at that moment.
_STORED_CALLER_DATA_NO_READ_SEAM: dict[str, str] = {
    "register_actor_config": (
        "the caller's metadata dict is stored by reference, but nothing reads "
        "it back (stored for future use) — a read seam added later must copy"
    ),
    "register_actor_configs": "same storage path as register_actor_config",
}

#: Test wiring whose sharing with the backend is the contract, not a defect.
_WIRING_SHARED_BY_DESIGN: dict[str, str] = {
    "subscribe_wake": "returns the subscriber event handle the backend sets — sharing is the mechanism",
    "subscribe_cancel_wake": "same subscriber-event contract as subscribe_wake",
    "register_cancel_event": "registers the caller's event for the backend to set — sharing is the point",
    "register_stub": "installs a caller-owned stub double — the stub must be the object the caller holds",
    "slot_table": "test-only handle exposing the internal slot table deliberately",
    "advance_clock_to": "drives the FakeClock; no state returned",
    "run_until_drained": "test driver; no state returned",
    "tick_cancel_polling": "test driver; no state returned",
}


def _public_members() -> dict[str, object]:
    return {
        name: value
        for name, value in inspect.getmembers(InMemoryBackend)
        if not name.startswith("_") and (inspect.isfunction(value) or isinstance(value, property))
    }


def _classified() -> dict[str, object]:
    """The union of every bucket's keys - only the names matter."""
    out: dict[str, object] = {}
    for bucket in (
        _PINNED_BY_TEST,
        _NO_STORED_STATE_ESCAPES,
        _STORED_CALLER_DATA_NO_READ_SEAM,
        _WIRING_SHARED_BY_DESIGN,
    ):
        out.update(bucket)
    out.update(dict.fromkeys(_PROBED_IN_THIS_FILE, "probed in this file"))
    return out


def test_every_public_member_is_classified_exactly_once() -> None:
    """The surface walk: a new public method on InMemoryBackend fails here
    until someone writes its classification - the sentence is the review."""
    members = set(_public_members())
    classified = _classified()
    unclassified = members - set(classified)
    stale = set(classified) - members
    assert not unclassified, (
        f"public InMemoryBackend members with no aliasing classification: {sorted(unclassified)}. "
        "Classify each into a bucket of tests/test_in_memory_seam_registry.py - "
        "if it returns rows or accepts caller-held mutables, that means adding "
        "an isolation test, not just a registry line."
    )
    assert not stale, (
        f"registry entries for members that no longer exist: {sorted(stale)}. "
        "Remove the entry when the method goes; the entry is a claim about the surface."
    )


def test_no_stored_state_bucket_annotations_name_no_row_type() -> None:
    """Tripwire on the exempt bucket: a method reclassified by an edit that
    makes it return a row type fails here until it is re-registered with a
    real isolation pin."""
    members = _public_members()
    for name in _NO_STORED_STATE_ESCAPES:
        member = members[name]
        assert inspect.isfunction(member), f"{name}: expected a method"
        annotation = str(inspect.signature(member).return_annotation)
        for row_type in _ROW_TYPE_NAMES:
            assert row_type not in annotation, (
                f"{name} is registered as 'no stored state escapes' but its return "
                f"annotation is {annotation!r}, which names {row_type}. The edit "
                "changed the contract; the registration must move to a pinned bucket "
                "with a real isolation test."
            )


def test_named_pins_exist_and_cover_the_method() -> None:
    """Every pinned entry names a test that exists; mention=True entries must
    also drive the method in the pin's source, so a gutted or renamed pin
    fails here rather than silently decoupling from the seam it guards."""
    for method, (module_name, test_name, must_mention) in _PINNED_BY_TEST.items():
        module = import_module(module_name)
        test_fn: Callable[..., object] | None = getattr(module, test_name, None)
        assert test_fn is not None, (
            f"{method} is registered as pinned by {module_name}::{test_name}, "
            "which does not exist. Re-pin the seam or fix the registry entry."
        )
        if must_mention:
            assert method in inspect.getsource(test_fn), (
                f"{module_name}::{test_name} no longer drives {method}(). "
                "The pin decoupled from the seam; restore the probe or re-register."
            )


# ── Behavioural probes for the seams this file owns ─────────────────────


def _make_backend() -> InMemoryBackend:
    return InMemoryBackend(clock=FakeClock(_START))


def _args(payload: dict[str, object]) -> EnqueueArgs:
    return EnqueueArgs(
        id=new_job_id(),
        actor="test_actor",
        queue="default",
        payload=payload,
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=_START,
        metadata={},
    )


async def _stored_payload(backend: InMemoryBackend, job_id: JobId) -> dict[str, object]:
    fresh = await backend.get(job_id)
    assert fresh is not None
    return fresh.payload


async def test_get_returned_row_does_not_alias_storage() -> None:
    """get is the seam every isolation test asserts through; pin it directly
    so a regression in _read_copy fails here with get's own name on it."""
    backend = _make_backend()
    args = _args({"key": "value"})
    await backend.enqueue(args)

    first = await backend.get(args.id)
    assert first is not None
    first.payload["injected"] = True

    assert "injected" not in await _stored_payload(backend, args.id)


async def test_enqueue_with_conn_matches_enqueue_isolation() -> None:
    """enqueue_with_conn delegates to the pinned _enqueue builder; the probe
    keeps the delegation honest at the behaviour level, not the call graph."""
    backend = _make_backend()
    payload: dict[str, object] = {"key": "value"}
    args = _args(payload)
    returned = await backend.enqueue_with_conn(None, args)

    payload["injected_caller"] = True
    returned.payload["injected_returned"] = True

    stored = await _stored_payload(backend, args.id)
    assert "injected_caller" not in stored
    assert "injected_returned" not in stored


async def test_enqueue_batch_returned_rows_do_not_alias_storage() -> None:
    backend = _make_backend()
    payload: dict[str, object] = {"key": "value"}
    args = _args(payload)
    rows = await backend.enqueue_batch([args])

    payload["injected_caller"] = True
    rows[0].payload["injected_returned"] = True

    stored = await _stored_payload(backend, args.id)
    assert "injected_caller" not in stored
    assert "injected_returned" not in stored


async def test_enqueue_batch_fast_does_not_store_caller_payload_by_reference() -> None:
    backend = _make_backend()
    payload: dict[str, object] = {"key": "value"}
    args = _args(payload)
    count = await backend.enqueue_batch_fast([args])
    assert count == 1

    payload["injected"] = True

    assert "injected" not in await _stored_payload(backend, args.id)


async def test_enqueue_batch_atomic_returned_rows_do_not_alias_storage() -> None:
    backend = _make_backend()
    payload: dict[str, object] = {"key": "value"}
    args = _args(payload)
    rows = await backend.enqueue_batch_atomic(
        [args],
        batch_id=new_uuid(),
        queue="default",
        batch_row=None,
        finalizer_args=None,
    )

    payload["injected_caller"] = True
    rows[0].payload["injected_returned"] = True

    stored = await _stored_payload(backend, args.id)
    assert "injected_caller" not in stored
    assert "injected_returned" not in stored

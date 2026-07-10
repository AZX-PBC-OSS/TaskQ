"""Tests for the Backend protocol, data carriers, and BACKEND_PROTOCOL_VERSION.

Covers the Definition of Done items:
- Protocol is @runtime_checkable
- BACKEND_PROTOCOL_VERSION == 2
- All 33 public members present (30 async methods + 2 sync
  subscribe_wake/subscribe_cancel_wake + supports_transactional_simulation)
- Five bool-returning terminal-write methods have bool return annotations
- mark_snoozed returns tri-state Literal; mark_retry_after returns cause-specific failed Literal
- mark_failed_or_retry returns JobRow; write_attempt returns None
- Each dataclass round-trips through dataclasses.asdict / construction
"""

from contextlib import AbstractAsyncContextManager as AsyncContextManager
from dataclasses import MISSING, asdict, fields
from datetime import UTC, datetime
from typing import Literal, get_type_hints

import pytest

from taskq._ids import new_uuid
from taskq.backend import (
    BACKEND_PROTOCOL_VERSION,
    AttemptOutcome,
    AttemptRow,
    Backend,
    CancelFlag,
    EnqueueArgs,
    ErrorInfo,
    JobFilter,
    JobPage,
    JobRow,
    JobStatus,
)
from taskq.backend._protocol import CancelPhase, JobId

# ── Helpers ────────────────────────────────────────────────────────────

_FIXED_UUID = new_uuid()
_NOW = datetime(2025, 1, 1, 0, 0, 0, tzinfo=UTC)

_ENQUEUE_DEFAULTS: dict[str, object] = {
    "id": _FIXED_UUID,
    "actor": "test_actor",
    "queue": "default",
    "payload": {"key": "value"},
    "max_attempts": 3,
    "retry_kind": "transient",
    "scheduled_at": _NOW,
}


def _make_enqueue_args(**overrides: object) -> EnqueueArgs:
    """Build a valid EnqueueArgs, applying keyword overrides."""
    merged = {**_ENQUEUE_DEFAULTS, **overrides}
    return EnqueueArgs(**merged)  # type: ignore[arg-type] # Why: override dict has broader value types than field annotations


_JOB_ROW_DEFAULTS: dict[str, object] = {
    "id": _FIXED_UUID,
    "actor": "test_actor",
    "queue": "default",
    "identity_key": None,
    "fairness_key": None,
    "payload": {"key": "value"},
    "payload_schema_ver": 1,
    "status": "pending",
    "priority": 0,
    "attempt": 0,
    "max_attempts": 3,
    "retry_kind": "transient",
    "schedule_to_close": None,
    "start_to_close": None,
    "heartbeat_timeout": None,
    "created_at": _NOW,
    "scheduled_at": _NOW,
    "started_at": None,
    "finished_at": None,
    "last_heartbeat_at": None,
    "locked_by_worker": None,
    "lock_expires_at": None,
    "cancel_requested_at": None,
    "cancel_phase": 0,
    "error_class": None,
    "error_message": None,
    "error_traceback": None,
    "progress_state": dict[str, object](),
    "progress_seq": 0,
    "result": None,
    "result_size_bytes": None,
    "result_expires_at": None,
    "idempotency_key": None,
    "trace_id": None,
    "span_id": None,
    "metadata": dict[str, object](),
    "tags": (),
}


def _make_job_row(**overrides: object) -> JobRow:
    """Build a valid JobRow, applying keyword overrides."""
    merged = {**_JOB_ROW_DEFAULTS, **overrides}
    return JobRow(**merged)  # type: ignore[arg-type] # Why: override dict has broader value types than field annotations


_ATTEMPT_ROW_DEFAULTS: dict[str, object] = {
    "job_id": _FIXED_UUID,
    "attempt": 1,
    "started_at": _NOW,
    "finished_at": _NOW,
    "outcome": "succeeded",
    "error_class": None,
    "error_message": None,
    "error_traceback": None,
    "duration_ms": 100,
    "worker_id": _FIXED_UUID,
    "metadata": dict[str, object](),
}


def _make_attempt_row(**overrides: object) -> AttemptRow:
    """Build a valid AttemptRow, applying keyword overrides."""
    merged = {**_ATTEMPT_ROW_DEFAULTS, **overrides}
    return AttemptRow(**merged)  # type: ignore[arg-type] # Why: override dict has broader value types than field annotations


# ── BACKEND_PROTOCOL_VERSION ────────────────────────────────────────────


class TestProtocolVersion:
    def test_version_is_two(self) -> None:
        assert BACKEND_PROTOCOL_VERSION == 2

    def test_version_is_int(self) -> None:
        assert isinstance(BACKEND_PROTOCOL_VERSION, int)


# ── B-TG-10: BACKEND_PROTOCOL_VERSION consistency across three modules ────


class TestProtocolVersionConsistency:
    """B-TG-10: all three protocol-implementing modules must declare
    BACKEND_PROTOCOL_VERSION == 2. Importing from each module's namespace
    guards against accidental shadowing or divergence on future bumps.
    """

    def test_protocol_module_version_is_two(self) -> None:
        import taskq.backend._protocol as proto_mod

        assert proto_mod.BACKEND_PROTOCOL_VERSION == 2

    def test_postgres_module_version_is_two(self) -> None:
        import taskq.backend.postgres as pg_mod

        assert pg_mod.BACKEND_PROTOCOL_VERSION == 2

    def test_in_memory_module_version_is_two(self) -> None:
        import taskq.testing.in_memory as mem_mod

        assert mem_mod.BACKEND_PROTOCOL_VERSION == 2

    def test_all_three_versions_equal(self) -> None:
        import taskq.backend._protocol as proto_mod
        import taskq.backend.postgres as pg_mod
        import taskq.testing.in_memory as mem_mod

        versions = {
            "protocol": proto_mod.BACKEND_PROTOCOL_VERSION,
            "postgres": pg_mod.BACKEND_PROTOCOL_VERSION,
            "in_memory": mem_mod.BACKEND_PROTOCOL_VERSION,
        }
        assert len(set(versions.values())) == 1, (
            f"BACKEND_PROTOCOL_VERSION diverged across modules: {versions}"
        )


# ── Runtime checkability ───────────────────────────────────────────────


class TestRuntimeCheckable:
    def test_isinstance_dummy_object_is_false(self) -> None:
        assert not isinstance(object(), Backend)

    def test_isinstance_dict_is_false(self) -> None:
        assert not isinstance({}, Backend)


# ── Protocol method count ──────────────────────────────────────────────


class TestMethodCount:
    def test_exactly_thirty_four_public_members(self) -> None:
        public = [m for m in dir(Backend) if not m.startswith("_")]
        assert len(public) == 34, f"Expected 34 public members, got {len(public)}: {public}"

    def test_all_member_names_present(self) -> None:
        expected = {
            "enqueue",
            "enqueue_batch",
            "enqueue_batch_fast",
            "enqueue_with_conn",
            "supports_transactional_simulation",
            "dispatch_batch",
            "heartbeat_jobs",
            "extend_reservation_leases",
            "mark_succeeded",
            "mark_succeeded_with_conn",
            "mark_failed_or_retry",
            "mark_cancelled",
            "write_cancel_escalation",
            "mark_abandoned",
            "mark_snoozed",
            "mark_retry_after",
            "retry_job",
            "write_attempt",
            "get_attempts",
            "get_events",
            "write_cancel_request",
            "poll_cancel_flags",
            "scheduled_to_pending",
            "deadline_sweep",
            "reclaim_expired_locks",
            "get",
            "list_jobs",
            "count_pending_jobs",
            "subscribe_wake",
            "subscribe_cancel_wake",
            "create_schedule",
            "list_schedules",
            "update_schedule",
            "delete_schedule",
        }
        actual = {m for m in dir(Backend) if not m.startswith("_")}
        assert actual == expected


# ── Return-type annotations ────────────────────────────────────────────


class TestReturnAnnotations:
    """Four bool-returning terminal writes; mark_snoozed and mark_retry_after
    return tri-state; mark_failed_or_retry -> JobRow; write_attempt -> None.
    """

    BOOL_METHODS = frozenset(
        {
            "mark_succeeded",
            "mark_succeeded_with_conn",
            "mark_cancelled",
            "write_cancel_escalation",
            "mark_abandoned",
        }
    )

    def test_bool_returning_methods(self) -> None:
        import asyncpg

        for name in self.BOOL_METHODS:
            fn = getattr(Backend, name)
            hints = get_type_hints(fn, globalns={**globals(), "asyncpg": asyncpg})
            assert hints.get("return") is bool, (
                f"{name} return annotation should be bool, got {hints.get('return')}"
            )

    def test_mark_failed_or_retry_returns_job_row(self) -> None:
        hints = get_type_hints(Backend.mark_failed_or_retry)
        assert hints.get("return") is JobRow, (
            f"mark_failed_or_retry should return JobRow, got {hints.get('return')}"
        )

    def test_mark_snoozed_returns_tri_state(self) -> None:
        from typing import get_args

        hints = get_type_hints(Backend.mark_snoozed)
        ret = hints.get("return")
        assert ret is not None and set(get_args(ret)) == {"scheduled", "failed", "noop"}, (
            f"mark_snoozed should return Literal['scheduled', 'failed', 'noop'], got {ret}"
        )

    def test_mark_retry_after_returns_tri_state(self) -> None:
        from typing import get_args

        hints = get_type_hints(Backend.mark_retry_after)
        ret = hints.get("return")
        expected = {
            "scheduled",
            "failed:DeadlineExceeded",
            "failed:MaxAttemptsExceeded",
            "noop",
        }
        assert ret is not None and set(get_args(ret)) == expected, (
            f"mark_retry_after should return cause-specific failed literals, got {ret}"
        )

    def test_write_attempt_returns_none(self) -> None:
        hints = get_type_hints(Backend.write_attempt)
        assert hints.get("return") is type(None), (
            f"write_attempt should return None, got {hints.get('return')}"
        )


# ── subscribe_wake is sync, returns AsyncContextManager[Event] ──────────


class TestSubscribeWakeSignature:
    def test_is_not_coroutine_function(self) -> None:
        import inspect

        assert not inspect.iscoroutinefunction(Backend.subscribe_wake)

    def test_return_annotation(self) -> None:
        hints = get_type_hints(Backend.subscribe_wake)
        ret = hints.get("return")
        assert ret is not None
        origin = getattr(ret, "__origin__", None)
        assert origin is AsyncContextManager, (
            f"subscribe_wake return origin should be AsyncContextManager, got {origin}"
        )


# ── Type aliases ────────────────────────────────────────────────────────


class TestTypeAliases:
    def test_job_status_literal_values(self) -> None:
        expected = {
            "pending",
            "scheduled",
            "running",
            "succeeded",
            "failed",
            "cancelled",
            "crashed",
            "abandoned",
        }
        # PEP 695 type aliases wrap the value in TypeAliasType; access
        # the underlying Literal via.__value__ before reading.__args__.
        args = set(JobStatus.__value__.__args__)  # type: ignore[attr-defined] # Why: PEP 695 type alias introspection
        assert args == expected

    def test_attempt_outcome_literal_values(self) -> None:
        expected = {
            "succeeded",
            "failed",
            "snoozed",
            "cancelled",
            "crashed",
            "reservation_denied",
            "rate_limit_denied",
        }
        args = set(AttemptOutcome.__value__.__args__)  # type: ignore[attr-defined] # Why: PEP 695 type alias introspection
        assert args == expected


# ── Dataclass round-trip tests ──────────────────────────────────────────


class TestEnqueueArgsRoundTrip:
    def test_construction_and_asdict(self) -> None:
        args = _make_enqueue_args()
        d = asdict(args)
        rebuilt = EnqueueArgs(**d)
        assert rebuilt == args

    def test_scheduled_at_has_no_default(self) -> None:
        """scheduled_at must be explicitly provided -- no dataclass default."""
        flds = {f.name: f for f in fields(EnqueueArgs)}
        assert flds["scheduled_at"].default is MISSING
        assert flds["scheduled_at"].default_factory is MISSING

    def test_field_count(self) -> None:
        expected = 24
        assert len(fields(EnqueueArgs)) == expected

    def test_frozen(self) -> None:
        args = _make_enqueue_args()
        with pytest.raises(AttributeError):
            args.actor = "changed"  # type: ignore[misc] # Why: deliberately mutating frozen dataclass to test immutability

    def test_optional_fields_accept_none(self) -> None:
        args = _make_enqueue_args(
            identity_key=None,
            fairness_key=None,
            idempotency_key=None,
            trace_id=None,
            span_id=None,
            schedule_to_close=None,
            start_to_close=None,
            heartbeat_timeout=None,
        )
        d = asdict(args)
        for key in (
            "identity_key",
            "fairness_key",
            "idempotency_key",
            "trace_id",
            "span_id",
            "schedule_to_close",
            "start_to_close",
            "heartbeat_timeout",
        ):
            assert d[key] is None


class TestJobRowRoundTrip:
    def test_construction_and_asdict(self) -> None:
        row = _make_job_row()
        d = asdict(row)
        rebuilt = JobRow(**d)
        assert rebuilt == row

    def test_status_literal_type(self) -> None:
        """status field annotation must be the JobStatus Literal, not str."""
        flds = {f.name: f for f in fields(JobRow)}
        assert flds["status"].type is JobStatus

    def test_field_count(self) -> None:
        expected = 37  # field list + tags
        assert len(fields(JobRow)) == expected

    def test_frozen(self) -> None:
        row = _make_job_row()
        with pytest.raises(AttributeError):
            row.status = "succeeded"  # type: ignore[misc] # Why: deliberately mutating frozen dataclass to test immutability

    def test_all_job_status_values_round_trip(self) -> None:
        for status in JobStatus.__value__.__args__:  # type: ignore[attr-defined] # Why: PEP 695 type alias introspection
            row = _make_job_row(status=str(status))  # type: ignore[arg-type] # Why: str(status) satisfies JobStatus at runtime
            assert row.status == status
            d = asdict(row)
            assert JobRow(**d) == row


class TestAttemptRowRoundTrip:
    def test_construction_and_asdict(self) -> None:
        row = _make_attempt_row()
        d = asdict(row)
        rebuilt = AttemptRow(**d)
        assert rebuilt == row

    def test_outcome_literal_type(self) -> None:
        flds = {f.name: f for f in fields(AttemptRow)}
        assert flds["outcome"].type is AttemptOutcome

    def test_field_count(self) -> None:
        expected = 11  # field list
        assert len(fields(AttemptRow)) == expected

    def test_frozen(self) -> None:
        row = _make_attempt_row()
        with pytest.raises(AttributeError):
            row.outcome = "failed"  # type: ignore[misc] # Why: deliberately mutating frozen dataclass to test immutability


class TestCancelFlagRoundTrip:
    def test_construction_and_asdict(self) -> None:
        flag = CancelFlag(job_id=JobId(_FIXED_UUID), cancel_phase=CancelPhase.COOPERATIVE)
        d = asdict(flag)
        rebuilt = CancelFlag(**d)
        assert rebuilt == flag

    def test_exactly_two_fields(self) -> None:
        names = {f.name for f in fields(CancelFlag)}
        assert names == {"job_id", "cancel_phase"}

    def test_frozen(self) -> None:
        flag = CancelFlag(job_id=JobId(_FIXED_UUID), cancel_phase=CancelPhase.COOPERATIVE)
        with pytest.raises(AttributeError):
            flag.cancel_phase = 2  # type: ignore[misc] # Why: deliberately mutating frozen dataclass to test immutability


class TestJobFilterRoundTrip:
    def test_construction_with_defaults(self) -> None:
        f = JobFilter()
        d = asdict(f)
        rebuilt = JobFilter(**d)
        assert rebuilt == f

    def test_defaults_are_none_or_limit(self) -> None:
        f = JobFilter()
        assert f.queue is None
        assert f.status is None
        assert f.actor is None
        assert f.identity_key is None
        assert f.batch_id is None
        assert f.limit == 100
        assert f.cursor is None

    def test_field_count(self) -> None:
        expected = 9
        assert len(fields(JobFilter)) == expected

    def test_frozen(self) -> None:
        f = JobFilter()
        with pytest.raises(AttributeError):
            f.queue = "q1"  # type: ignore[misc] # Why: deliberately mutating frozen dataclass to test immutability


class TestErrorInfoRoundTrip:
    def test_construction_and_asdict(self) -> None:
        info = ErrorInfo(
            error_class="ValueError",
            error_message="bad input",
            error_traceback=None,
        )
        d = asdict(info)
        rebuilt = ErrorInfo(**d)
        assert rebuilt == info

    def test_field_count(self) -> None:
        expected = 3
        assert len(fields(ErrorInfo)) == expected

    def test_frozen(self) -> None:
        info = ErrorInfo(
            error_class="ValueError",
            error_message="bad input",
            error_traceback=None,
        )
        with pytest.raises(AttributeError):
            info.error_class = "TypeError"  # type: ignore[misc] # Why: deliberately mutating frozen dataclass to test immutability


class TestJobPageRoundTrip:
    def test_construction_and_asdict(self) -> None:
        row = _make_job_row()
        page = JobPage(jobs=[row], next_cursor=None)
        d = asdict(page)
        # asdict recursively converts nested dataclasses to dicts, so
        # JobPage(**d) would produce jobs=list[dict] instead of list[JobRow].
        # Reconstruct each JobRow from its dict representation.
        rebuilt = JobPage(
            jobs=[JobRow(**j) for j in d["jobs"]],  # type: ignore[arg-type] # Why: asdict produces dicts; runtime types are correct
            next_cursor=d["next_cursor"],
        )
        assert rebuilt == page

    def test_field_count(self) -> None:
        expected = 2
        assert len(fields(JobPage)) == expected

    def test_frozen(self) -> None:
        page = JobPage(jobs=[], next_cursor=None)
        with pytest.raises(AttributeError):
            page.next_cursor = "abc"  # type: ignore[misc] # Why: deliberately mutating frozen dataclass to test immutability


# ── No forbidden imports ───────────────────────────────────────────────


class TestNoForbiddenImports:
    def test_backend_module_does_not_import_pytest(self) -> None:
        import taskq.backend as backend_mod

        assert "pytest" not in dir(backend_mod)

    def test_backend_module_does_not_import_testcontainers(self) -> None:
        import sys

        mod = sys.modules.get("taskq.backend")
        assert mod is not None
        assert "testcontainers" not in dir(mod)

    def test_backend_module_does_not_import_redis(self) -> None:
        import sys

        mod = sys.modules.get("taskq.backend")
        assert mod is not None
        assert "redis" not in dir(mod)

    def test_no_future_annotations(self) -> None:
        """no ``from __future__ import annotations`` in backend."""
        import taskq.backend as backend_mod
        import taskq.backend._protocol as proto_mod

        for mod in (backend_mod, proto_mod):
            source_file = mod.__spec__.origin  # type: ignore[attr-defined] # Why: ModuleSpec.origin exists at runtime but type stubs omit it
            assert source_file is not None
            with open(source_file) as f:
                for line in f:
                    assert "from __future__ import annotations" not in line

    def test_no_any_at_protocol_boundary(self) -> None:
        """no ``Any`` import in the backend module."""
        import taskq.backend as backend_mod
        import taskq.backend._protocol as proto_mod

        assert not hasattr(backend_mod, "Any")
        assert not hasattr(proto_mod, "Any")
        for mod in (backend_mod, proto_mod):
            source_file = mod.__spec__.origin  # type: ignore[attr-defined] # Why: ModuleSpec.origin exists at runtime but type stubs omit it
            assert source_file is not None
            with open(source_file) as f:
                for line in f:
                    assert "Any" not in line or "dict[str, object]" in line


# ── write_cancel_escalation phase annotation ────────────────────────────


class TestWriteCancelEscalationPhaseLiteral:
    def test_phase_is_literal_two(self) -> None:
        hints = get_type_hints(Backend.write_cancel_escalation)
        phase_type = hints.get("phase")
        assert phase_type is not None
        origin = getattr(phase_type, "__origin__", None)
        assert origin is Literal
        args = getattr(phase_type, "__args__", ())
        assert args == (2,)

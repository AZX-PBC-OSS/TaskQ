"""Coverage for ``taskq.backend.__init__`` lazy ``__getattr__`` re-exports.

The backend package re-exports protocol definitions eagerly and
``PostgresBackend`` lazily via ``__getattr__`` (to avoid importing
``asyncpg`` at package import time).  These tests verify the lazy
``PostgresBackend`` path resolves, unknown names raise ``AttributeError``,
and every name in ``__all__`` is importable from the package.
"""

import importlib

import pytest

import taskq.backend
from taskq.backend import (
    BACKEND_PROTOCOL_VERSION,
    TERMINAL_STATUSES,
    VALID_TRANSITIONS,
    AttemptOutcome,
    AttemptRow,
    Backend,
    BackendDeps,
    CancelFlag,
    DstStrategy,
    EnqueueArgs,
    ErrorInfo,
    JobFilter,
    JobPage,
    JobRow,
    JobStatus,
    ScheduleRecord,
    assert_valid_transition,
    decode_cursor,
    encode_cursor,
)


def test_postgres_backend_resolves_lazily() -> None:
    """``PostgresBackend`` resolves via ``__getattr__`` to the real class."""
    from taskq.backend.postgres import PostgresBackend

    resolved = taskq.backend.PostgresBackend  # type: ignore[attr-defined]  # Why: triggers __getattr__ lazy import path.
    assert resolved is PostgresBackend


def test_unknown_attribute_raises_attribute_error() -> None:
    """An attribute not provided by ``__getattr__`` raises AttributeError."""
    with pytest.raises(AttributeError, match=r"has no attribute"):
        _ = taskq.backend.does_not_exist  # type: ignore[attr-defined]


def test_all_names_importable() -> None:
    """Every name in ``__all__`` can be fetched from the package."""
    for name in taskq.backend.__all__:
        obj = getattr(taskq.backend, name)
        assert obj is not None, f"taskq.backend.{name} resolved to None"


def test_eager_reexports_match_submodules() -> None:
    """Eagerly imported names match their source submodule objects."""
    pairs: list[tuple[object, str, str]] = [
        (BACKEND_PROTOCOL_VERSION, "taskq.backend._protocol", "BACKEND_PROTOCOL_VERSION"),
        (AttemptOutcome, "taskq.backend._protocol", "AttemptOutcome"),
        (AttemptRow, "taskq.backend._protocol", "AttemptRow"),
        (Backend, "taskq.backend._protocol", "Backend"),
        (BackendDeps, "taskq.backend._protocol", "BackendDeps"),
        (CancelFlag, "taskq.backend._protocol", "CancelFlag"),
        (DstStrategy, "taskq.backend._protocol", "DstStrategy"),
        (EnqueueArgs, "taskq.backend._protocol", "EnqueueArgs"),
        (ErrorInfo, "taskq.backend._protocol", "ErrorInfo"),
        (JobFilter, "taskq.backend._protocol", "JobFilter"),
        (JobPage, "taskq.backend._protocol", "JobPage"),
        (JobRow, "taskq.backend._protocol", "JobRow"),
        (JobStatus, "taskq.backend._protocol", "JobStatus"),
        (ScheduleRecord, "taskq.backend._protocol", "ScheduleRecord"),
        (decode_cursor, "taskq.backend._cursor", "decode_cursor"),
        (encode_cursor, "taskq.backend._cursor", "encode_cursor"),
        (TERMINAL_STATUSES, "taskq.backend.statemachine", "TERMINAL_STATUSES"),
        (VALID_TRANSITIONS, "taskq.backend.statemachine", "VALID_TRANSITIONS"),
        (assert_valid_transition, "taskq.backend.statemachine", "assert_valid_transition"),
    ]
    for obj, module_name, attr in pairs:
        direct = getattr(importlib.import_module(module_name), attr)
        assert obj is direct, f"{attr} from {module_name} differs from package re-export"

"""Backend protocol and data carriers for TaskQ.

Re-exports the :class:`Backend` protocol, dataclass carriers, and
``BACKEND_PROTOCOL_VERSION`` from :mod:`taskq.backend._protocol`, and
the concrete :class:`PostgresBackend` from :mod:`taskq.backend.postgres`.
Also re-exports state machine constants and the transition guard from
:mod:`taskq.backend.statemachine`.

The protocol definitions live in a dedicated submodule so that concrete
backend implementations can import them without creating a circular
dependency through this re-export boundary.
"""

# pyright: reportUnsupportedDunderAll=false

from taskq.backend._cursor import decode_cursor, encode_cursor
from taskq.backend._protocol import (
    BACKEND_PROTOCOL_VERSION,
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
)
from taskq.backend.statemachine import (
    TERMINAL_STATUSES,
    VALID_TRANSITIONS,
    assert_valid_transition,
)


def __getattr__(name: str) -> object:
    if name == "PostgresBackend":
        from taskq.backend.postgres import PostgresBackend

        return PostgresBackend
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [  # pyright: ignore[reportUnsupportedDunderAll]  # Why: __getattr__ lazily provides PostgresBackend at module level
    "BACKEND_PROTOCOL_VERSION",
    "TERMINAL_STATUSES",
    "VALID_TRANSITIONS",
    "AttemptOutcome",
    "AttemptRow",
    "Backend",
    "BackendDeps",
    "CancelFlag",
    "DstStrategy",
    "EnqueueArgs",
    "ErrorInfo",
    "JobFilter",
    "JobPage",
    "JobRow",
    "JobStatus",
    "PostgresBackend",
    "ScheduleRecord",
    "assert_valid_transition",
    "decode_cursor",
    "encode_cursor",
]

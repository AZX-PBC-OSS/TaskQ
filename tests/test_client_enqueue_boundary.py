"""Client enqueue-boundary contracts: deprecation-warning attribution and
input validation.

Two redteam findings live here:

- The ``schedule_to_close`` DeprecationWarning must blame the USER's call
  line, not taskq internals.  A static stacklevel cannot serve both public
  entries — the user's line is 3 frames above ``build_enqueue_args`` via
  ``JobsClient.enqueue`` and 4 via the ``TaskQ.enqueue`` facade — so the
  warning walks to the first frame outside the taskq package.
- Naive datetimes must be rejected for ``scheduled_at`` /
  ``schedule_to_close`` (docs already claim "naive datetimes are not
  accepted at the backend boundary"), and negative ``result_ttl`` /
  ``schedule_to_close_interval`` must be rejected — mirroring
  ``start_to_close``'s boundary checks.
"""

import sys
import warnings
from datetime import UTC, datetime, timedelta

import pytest
from pydantic import BaseModel

from taskq.actor import actor
from taskq.backend.clock import SystemClock
from taskq.client._jobs import JobsClient
from taskq.retry import RetryPolicy
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend

_START = datetime(2025, 1, 1, tzinfo=UTC)
_FUTURE_DEADLINE = _START + timedelta(minutes=10)


class _Payload(BaseModel):
    value: int = 0


@actor(name="client_boundary_actor")
async def _boundary_actor(_payload: _Payload) -> None:
    pass


@actor(name="client_boundary_neg_result_ttl_actor", result_ttl=timedelta(seconds=-1))
async def _neg_result_ttl_actor(_payload: _Payload) -> None:
    pass


@actor(
    name="client_boundary_neg_budget_actor",
    retry=RetryPolicy(kind="indefinite", time_budget=timedelta(seconds=-1)),
)
async def _neg_budget_actor(_payload: _Payload) -> None:
    pass


def _make_client() -> JobsClient:
    return JobsClient(backend=InMemoryBackend(FakeClock(_START)), clock=SystemClock())


def _next_line() -> int:
    """Line number of the statement immediately after the call to this
    helper minus one — i.e. the line the caller places directly below it,
    which each test pins as the user's enqueue call line."""
    return sys._getframe(1).f_lineno + 1


# ── D2: the deprecation warning blames the user's call line ──────────────


async def test_schedule_to_close_warning_blames_jobs_client_caller() -> None:
    """Via the JobsClient.enqueue public entry (user → JobsClient.enqueue →
    build_enqueue_args → warn), the warning's reported location must be the
    USER's enqueue line — 3 frames up — not taskq/client/_jobs.py."""
    client = _make_client()
    with pytest.warns(DeprecationWarning) as record:
        expected_line = _next_line()  # the enqueue call below is the blamed frame
        await client.enqueue(_boundary_actor, _Payload(), schedule_to_close=_FUTURE_DEADLINE)
    assert len(record) == 1
    warning = record[0]
    assert warning.filename == __file__, (
        f"warning blamed {warning.filename!r} (taskq internals), not the user's module"
    )
    assert warning.lineno == expected_line, (
        f"warning blamed line {warning.lineno}, not the user's enqueue call line"
    )


async def test_schedule_to_close_warning_blames_taskq_facade_caller() -> None:
    """Via the TaskQ.enqueue facade (user → TaskQ.enqueue →
    JobsClient.enqueue → build_enqueue_args → warn) — one delegation frame
    deeper, 4 frames up — the warning must still blame the USER's line."""
    from taskq.client._taskq import TaskQ

    tq = TaskQ.__new__(TaskQ)  # Why: facade-only wiring; no pool is opened or used.
    tq._client = _make_client()  # pyright: ignore[reportPrivateUsage]  # Why: inject the real JobsClient so the genuine facade path runs.

    with pytest.warns(DeprecationWarning) as record:
        expected_line = _next_line()  # the enqueue call below is the blamed frame
        await tq.enqueue(_boundary_actor, _Payload(), schedule_to_close=_FUTURE_DEADLINE)
    assert len(record) == 1
    warning = record[0]
    assert warning.filename == __file__, (
        f"warning blamed {warning.filename!r} (taskq internals), not the user's module"
    )
    assert warning.lineno == expected_line, (
        f"warning blamed line {warning.lineno}, not the user's enqueue call line"
    )


# ── D3: naive datetimes and negative intervals are rejected ──────────────


async def test_naive_scheduled_at_rejected() -> None:
    """A naive (tz-unaware) scheduled_at raises ValueError at the client
    boundary — docs already claim naive datetimes are not accepted."""
    client = _make_client()
    with pytest.raises(ValueError, match=r"scheduled_at.*timezone-aware"):
        await client.enqueue(_boundary_actor, _Payload(), scheduled_at=datetime(2025, 1, 2))


async def test_naive_schedule_to_close_rejected() -> None:
    """A naive (tz-unaware) schedule_to_close raises ValueError at the
    client boundary (and is not merely warned about)."""
    client = _make_client()
    with (
        pytest.raises(ValueError, match=r"schedule_to_close.*timezone-aware"),
        warnings.catch_warnings(),
    ):
        warnings.simplefilter("error")  # Why: a naive value must be rejected, not warned.
        await client.enqueue(
            _boundary_actor,
            _Payload(),
            schedule_to_close=datetime(2025, 1, 2),
        )


async def test_negative_result_ttl_rejected() -> None:
    """An actor declaring a negative result_ttl is rejected at the enqueue
    boundary — validation existed only in actor-config ops before."""
    client = _make_client()
    with pytest.raises(ValueError, match=r"result_ttl.*non-negative"):
        await client.enqueue(_neg_result_ttl_actor, _Payload())


async def test_negative_schedule_to_close_interval_rejected() -> None:
    """An indefinite-tier actor with a negative retry.time_budget (the
    schedule_to_close_interval source) is rejected at the enqueue
    boundary."""
    client = _make_client()
    with pytest.raises(ValueError, match=r"schedule_to_close_interval.*non-negative"):
        await client.enqueue(_neg_budget_actor, _Payload())

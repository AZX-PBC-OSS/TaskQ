"""Regression: start_to_close must actually stop the actor on both paths.

The transactional path wrapped the actor in ``asyncio.shield()`` INSIDE the
``asyncio.wait_for()`` that enforces ``start_to_close``.  ``shield`` is
documented to leave the shielded awaitable running when the waiter is
cancelled, so the timeout fired, the attempt was marked timed out and became
eligible for a retry elsewhere — while the actor body kept executing.  Every
side effect after the timeout point ran twice.

The autonomous path has always cancelled correctly; it is the control here,
and the two paths must behave identically.
"""

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import replace as _dc_replace
from datetime import UTC, datetime, timedelta

import pytest
from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq.backend._protocol import AttemptOutcome, JobRow
from taskq.context import JobContext
from taskq.testing.actor import EmptyPayload, FakeBackend, as_backend, default_actor_config
from taskq.testing.clock import FakeClock
from taskq.testing.jobs import make_job_row
from taskq.worker._consumer import consume_one_job

_NOW = datetime(2025, 1, 1, tzinfo=UTC)
_WORKER_ID = new_uuid()
_TIMEOUT = timedelta(milliseconds=100)


class _FakeConnection:
    """Minimal asyncpg.Connection stand-in with a transaction() context manager."""

    class _Transaction:
        async def __aenter__(self) -> "_FakeConnection._Transaction":
            return self

        async def __aexit__(self, *args: object) -> None:
            return None

    def transaction(self) -> "_FakeConnection._Transaction":
        return self._Transaction()

    async def execute(self, query: str, *args: object) -> str:
        return ""


class _ActorProbe:
    """Records whether the actor was cancelled or ran to completion."""

    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.cancelled = False
        self.completed = False

    def run_actor(self) -> Callable[[JobRow, JobContext[BaseModel]], Awaitable[object]]:
        async def _run_actor(_job: JobRow, _ctx: JobContext[BaseModel]) -> object:
            self.started.set()
            try:
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                self.cancelled = True
                raise
            self.completed = True
            return {"ok": True}

        return _run_actor


async def _consume(probe: _ActorProbe, *, transactional: bool) -> AttemptOutcome | None:
    backend = FakeBackend()
    job = _dc_replace(
        make_job_row(start_to_close=_TIMEOUT),
        locked_by_worker=_WORKER_ID,
    )
    outcome: AttemptOutcome | None = None
    with suppress(asyncio.CancelledError):
        outcome = await consume_one_job(
            as_backend(backend),
            job,
            _WORKER_ID,
            run_actor=probe.run_actor(),
            actor_config=default_actor_config(),
            payload_type=EmptyPayload,
            clock=FakeClock(_NOW),
            loop_conn=_FakeConnection() if transactional else None,  # pyright: ignore[reportArgumentType]  # Why: structural asyncpg.Connection stand-in, as in tests/test_consumer_sub_enqueue.py
        )
    return outcome


@pytest.mark.parametrize("transactional", [True, False], ids=["transactional", "autonomous"])
async def test_start_to_close_timeout_cancels_the_actor(transactional: bool) -> None:
    """The actor must be cancelled by the timeout, not left running detached."""
    probe = _ActorProbe()

    await _consume(probe, transactional=transactional)

    assert probe.started.is_set()
    assert probe.cancelled, "start_to_close fired but the actor was never cancelled"

    # Nothing may resurrect it afterwards either: give a detached actor every
    # chance to keep running before declaring it stopped.
    await asyncio.sleep(0.05)
    assert not probe.completed


@pytest.mark.parametrize("transactional", [True, False], ids=["transactional", "autonomous"])
async def test_timeout_outcome_is_reported_on_both_paths(transactional: bool) -> None:
    """Both paths route the timeout through the failure dispatcher."""
    probe = _ActorProbe()

    outcome = await _consume(probe, transactional=transactional)

    assert outcome in ("failed", "scheduled"), outcome

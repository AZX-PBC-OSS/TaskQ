"""Failure-mode actors — retry, snooze, and simulated errors.

These actors demonstrate TaskQ's failure-handling primitives: automatic
retry with a configurable policy (flaky) and cooperative snooze/deferral
(snoozer).
"""

from datetime import timedelta

from pydantic import BaseModel

from taskq import JobContext, RetryPolicy, Snooze, actor


class FlakyPayload(BaseModel):
    fail_count: int = 2


class SnoozePayload(BaseModel):
    delay_seconds: int = 10
    wake_after_attempt: int = 1


@actor(name="flaky", queue="examples", retry=RetryPolicy(max_attempts=5))
async def flaky(payload: FlakyPayload, ctx: JobContext[FlakyPayload]) -> None:
    """Fails on the first N attempts, then succeeds — demonstrates retry."""
    if ctx.attempt <= payload.fail_count:
        raise ValueError("simulated failure")


@actor(name="snoozer", queue="examples")
async def snoozer(payload: SnoozePayload, ctx: JobContext[SnoozePayload]) -> None:
    """Snoozes for a configurable delay on the first attempt, then succeeds."""
    if ctx.attempt <= payload.wake_after_attempt:
        raise Snooze(timedelta(seconds=payload.delay_seconds))

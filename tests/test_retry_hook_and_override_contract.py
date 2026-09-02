"""Contract edges of ``RetryOverride.delay`` and the retry-exhausted hook.

Two behaviours that ran under test with nothing pinning them:

* ``RetryOverride.delay`` documents ``>= 0``; only the rejection of a
  negative value was asserted, so nothing said zero is *accepted*.
* ``invoke_on_retry_exhausted`` awaits the hook result only when it is
  actually awaitable.  Every covering test used an async hook, so the
  ``result is not None and inspect.isawaitable(result)`` guard was never
  exercised with a synchronous hook that returns a value — the case the
  ``isawaitable`` half exists for.  Awaiting a non-awaitable raises
  ``TypeError`` from ``asyncio.wait_for`` itself; the hook's own
  ``except Exception`` then swallows it, so the only visible trace is a
  spurious ``on-retry-exhausted-hook-failed`` warning against a hook that
  in fact ran and succeeded.  That warning is the assertable difference.
"""

from datetime import UTC, datetime, timedelta
from typing import cast

import pytest
import structlog.testing

from taskq._ids import new_uuid
from taskq.backend._protocol import JobRow
from taskq.backend.clock import Clock
from taskq.context import JobContext
from taskq.retry import (
    OnRetryExhausted,
    OnSuccess,
    RetryOverride,
    RetryPolicy,
    invoke_on_retry_exhausted,
    invoke_on_success,
)
from taskq.testing.actor import EmptyPayload, FakeBackend, StubActorConfig, as_backend
from taskq.testing.clock import FakeClock
from taskq.testing.jobs import make_job_row
from taskq.worker._consumer import consume_one_job

_NOW = datetime(2025, 1, 1, tzinfo=UTC)
_WORKER_ID = new_uuid()


# ── RetryOverride.delay: zero is a legal delay ───────────────────────


def test_retry_override_accepts_zero_delay() -> None:
    """``delay=timedelta(0)`` means "retry immediately", not "invalid"."""
    assert RetryOverride(delay=timedelta(0)).delay == timedelta(0)


def test_retry_override_rejects_negative_delay() -> None:
    """The other side of the same boundary, kept next to it."""
    with pytest.raises(ValueError, match="delay must be >= 0"):
        RetryOverride(delay=timedelta(microseconds=-1))


# ── on_retry_exhausted: a synchronous hook that returns a value ──────


def _sync_hook_returning_a_value(calls: list[str]) -> OnRetryExhausted:
    """A sync hook whose return value is not ``None`` and not awaitable.

    Why the cast: ``OnRetryExhausted`` is declared to return
    ``Awaitable[None] | None``, and this hook deliberately returns
    something else — that is precisely the off-contract shape the runtime
    ``isawaitable`` guard defends against.
    """

    def _hook(job_row: JobRow, exception: BaseException) -> str:
        calls.append(str(exception))
        return "done"

    return cast(OnRetryExhausted, _hook)


async def test_sync_hook_returning_value_is_not_awaited() -> None:
    """A non-awaitable return is dropped, not awaited — and not reported failed.

    Why the log assertion: awaiting the value raises ``TypeError``, which the
    hook's own ``except Exception`` swallows, so the run still *completes*.
    The only observable difference is the warning it emits — a hook that ran
    cleanly being reported as failed.
    """
    calls: list[str] = []
    job = make_job_row(actor="sync_hook_actor")

    with structlog.testing.capture_logs() as logs:
        await invoke_on_retry_exhausted(
            _sync_hook_returning_a_value(calls),
            job,
            RuntimeError("boom"),
            3.0,
        )

    assert calls == ["boom"]
    assert [entry["event"] for entry in logs] == []


async def test_consumer_completes_when_exhausted_hook_is_sync_and_returns_value() -> None:
    """Retry exhaustion with such a hook still reaches the terminal write.

    Why through the consumer: this is the call site the guard protects, and
    it pins that a legal terminal write still happens with such a hook
    registered.
    """
    calls: list[str] = []
    backend = FakeBackend()
    clock: Clock = FakeClock(_NOW)
    cfg = StubActorConfig(
        retry=RetryPolicy(kind="transient", max_attempts=3, jitter=0.0),
        on_retry_exhausted=_sync_hook_returning_a_value(calls),
    )
    job = make_job_row(actor="sync_hook_actor", attempt=3, max_attempts=3, retry_kind="transient")

    async def actor(_job: JobRow, _ctx: JobContext[EmptyPayload]) -> object:
        raise RuntimeError("boom")

    outcome = await consume_one_job(
        as_backend(backend),
        job,
        _WORKER_ID,
        run_actor=actor,
        actor_config=cfg,
        payload_type=EmptyPayload,
        clock=clock,
    )

    assert outcome == "failed"
    assert calls == ["boom"]
    assert len(backend.mark_failed_or_retry_calls) == 1
    assert backend.mark_failed_or_retry_calls[0]["retry_delay"] is None


# ── on_success: the identical guard, same blind spot ─────────────────


async def test_sync_on_success_hook_returning_value_is_not_awaited() -> None:
    """``invoke_on_success`` carries the same guard and had the same gap."""
    calls: list[object] = []

    def _hook(job_row: JobRow, result: object) -> str:
        calls.append(result)
        return "done"

    job = make_job_row(actor="sync_hook_actor")

    with structlog.testing.capture_logs() as logs:
        await invoke_on_success(cast(OnSuccess, _hook), job, {"ok": True}, 3.0)

    assert calls == [{"ok": True}]
    assert [entry["event"] for entry in logs] == []

"""A wait_for timeout on a sync actor must not re-pend the row while its
thread still runs.

Both timeout arms (the transactional ``wait_for`` and the autonomous one)
route the ``TimeoutError`` to ``_handle_timeout``, whose terminal write
re-pends the row (``mark_failed_or_retry``) for the retry decision. For a
sync ``def`` actor the wrapped executor thread cannot be cancelled: the
``wait_for`` cancel detaches the shield, the thread keeps executing, and the
row went straight back to claimable while the body was still mid-run, the
same overlap shape the shutdown release path closed for the interrupt path
, on the timeout path.

The fix routes the timeout handler through the SAME exit-proof hold the
interrupt path uses: the handler parks on the job's tracked exit handles
(``ctx._sync_actor_task``, ``ctx._tx_unwind_task``), bounded by
``_actor_exit_wait_budget``, and re-pends with the decision's own retry
delay only on a provable exit inside that window. A thread that outlives
the window is not provable: the re-pend is deferred behind the release hold
(the process's exit window, the same ``_release_hold`` computation), and the
promise is scoped in docs/architecture.md, a sync actor that outlives both
the park and that window can still overlap a re-claim, because a runtime
timeout has no watchdog deadline to anchor an enforced bound on.

This is the unit tier (no Postgres): the production dispatch helper
(``_run_sync_actor_tracked``) drives the tracked handle through the
production ``consume_one_job`` / ``_consume_transactional`` against the
recording ``FakeBackend``.
"""

from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import Awaitable, Callable, Iterator
from contextlib import suppress
from datetime import timedelta
from unittest.mock import MagicMock

import asyncpg
import pytest
import structlog
from opentelemetry import trace
from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq.backend._protocol import JobRow
from taskq.backend.clock import Clock
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.context import JobContext
from taskq.obs import bind_job_context
from taskq.settings import WorkerSettings
from taskq.testing.actor import (
    EmptyPayload,
    FakeBackend,
    as_backend,
    default_actor_config,
)
from taskq.testing.clock import FakeClock
from taskq.testing.jobs import make_job_row
from taskq.worker._consumer import consume_one_job
from taskq.worker.deps import WorkerDeps
from taskq.worker.dispatch import (  # pyright: ignore[reportPrivateUsage]  # Why: the production sync-actor dispatch helper: the tracking under test is exactly what dispatch_one_job calls for a sync actor.
    _run_sync_actor_tracked,
)
from taskq.worker.shutdown import (  # pyright: ignore[reportPrivateUsage]  # Why: the deferred re-pend's bound is the release hold the interrupt path carries; the test recomputes it from the same exit tail.
    _watchdog_exit_tail,
)

_NOW_FROZEN = None  # the clock is never advanced; real time drives the timeouts
_WORKER_ID = new_uuid()


@pytest.fixture(autouse=True)
def _clean_tracked_actor_handles() -> Iterator[None]:  # pyright: ignore[reportUnusedFunction]  # Why: autouse fixture consumed implicitly by the test runner; pyright does not track fixture usage.
    """Isolate the process-wide tracked-handle registry per test.

    The registry is module state shared with every other module in the
    session; a handle leaked by a failed assertion here would park a later
    ``await_tracked_actor_reap`` forever (the watchdog is its only bound,
    and unit tests do not arm one).
    """
    from taskq.worker import _watchdog as _watchdog_mod

    saved = set(_watchdog_mod._tracked_actor_handles)  # pyright: ignore[reportPrivateUsage]  # Why: test isolation of the module-level registry under assertion.
    _watchdog_mod._tracked_actor_handles.clear()
    try:
        yield
    finally:
        _watchdog_mod._tracked_actor_handles.clear()
        _watchdog_mod._tracked_actor_handles.update(saved)


def _settings(*, cleanup_grace: float, termination_grace: float = 20.0) -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {
            "TASKQ_SCHEMA_NAME": "taskq_test",
            "TASKQ_CANCELLATION_GRACE_PERIOD": "0.0",
            "TASKQ_CLEANUP_GRACE_PERIOD": str(cleanup_grace),
            "TASKQ_TERMINATION_GRACE_PERIOD": str(termination_grace),
        }
    )


def _deps_with(settings: WorkerSettings) -> MagicMock:
    deps = MagicMock(spec=WorkerDeps)
    deps.settings = settings
    deps.disowned_jobs = set()
    deps.progress_buffers = {}
    deps.worker_pool = None
    deps.redis_client = None
    deps.shutdown_started_at = None
    return deps


def _gated_body(
    body_started: threading.Event,
    release_body: threading.Event,
    body_returned: threading.Event,
) -> Callable[[EmptyPayload], dict[str, bool]]:
    def body(payload: EmptyPayload) -> dict[str, bool]:
        del payload
        body_started.set()
        release_body.wait(30.0)
        body_returned.set()
        return {"done": True}

    return body


def _tracked_sync_run_actor(
    body: Callable[..., object],
    captured_ctx: list[JobContext[EmptyPayload]],
) -> Callable[[JobRow, JobContext[EmptyPayload]], Awaitable[object]]:
    """Wrap a sync body in the production tracked dispatch helper, capturing
    the per-attempt context so the test can reach the tracked thread handle."""

    async def run_actor(job_row: JobRow, ctx: JobContext[EmptyPayload]) -> object:
        del job_row
        captured_ctx.append(ctx)
        return await _run_sync_actor_tracked(body, {"payload": ctx.payload}, ctx)

    return run_actor


# ── The autonomous timeout arm ───────────────────────────────────────────


async def test_a_sync_actor_that_outlives_its_timeout_is_not_repended_while_its_thread_runs() -> (
    None
):
    """The regression cell (autonomous arm): the re-pend waits for exit.

    The ``wait_for`` fires while the sync body is mid-run; the timeout
    handler must park on the tracked thread handle and write the retry
    decision only once the thread has provably exited. On the old arm the
    ``mark_failed_or_retry`` write landed immediately: the row was
    claimable by another worker while the executor thread still ran.
    """
    settings = _settings(cleanup_grace=2.0)
    deps = _deps_with(settings)
    backend = FakeBackend()
    job = make_job_row(start_to_close=timedelta(seconds=0.2))
    clock: Clock = FakeClock(_NOW_FROZEN)

    body_started = threading.Event()
    release_body = threading.Event()
    body_returned = threading.Event()
    captured_ctx: list[JobContext[EmptyPayload]] = []

    attempt = asyncio.ensure_future(
        consume_one_job(
            as_backend(backend),
            job,
            _WORKER_ID,
            deps=deps,  # type: ignore[arg-type]  # Why: MagicMock(spec=WorkerDeps) with the attrs the consumer reads set to real values.
            run_actor=_tracked_sync_run_actor(
                _gated_body(body_started, release_body, body_returned), captured_ctx
            ),
            actor_config=default_actor_config(),
            payload_type=EmptyPayload,
            clock=clock,
            settings=settings,
        )
    )
    await asyncio.to_thread(body_started.wait, 10.0)
    # The 0.2s wait_for fires while the body is still gated on release_body.
    await asyncio.sleep(0.45)

    assert not body_returned.is_set(), "the gated body must still be running past the timeout"
    assert backend.mark_failed_or_retry_calls == [], (
        "the row must NOT be re-pended while the sync actor's thread is "
        f"provably still alive; the timeout handler wrote {backend.mark_failed_or_retry_calls}"
    )

    # The body exits inside the 2.0s park window: the write lands after.
    release_body.set()
    with suppress(asyncio.CancelledError):
        await asyncio.wait_for(attempt, timeout=15.0)

    assert body_returned.is_set(), "the body must have exited before the write landed"
    assert len(backend.mark_failed_or_retry_calls) == 1, (
        "exactly one retry write, landed only after the provable exit"
    )
    handle = captured_ctx[0]._sync_actor_task  # pyright: ignore[reportPrivateUsage]  # Why: the test reads the dispatch layer's handle to join the executor thread before the test ends.
    assert handle is not None and handle.done(), (
        "the tracked handle is done once the thread returned"
    )


async def test_a_sync_actor_outliving_the_exit_park_repends_behind_the_release_hold() -> None:
    """A thread that outlives the bounded park is not provable: the re-pend
    is deferred behind the release hold, never silent.

    The park is bounded (``_actor_exit_wait_budget``); a body still running
    at its expiry cannot earn hold=0, so the retry delay must carry the
    same hold the interrupt path's release uses, the row is not claimable
    for the process's exit window. The scope is honest: this is a deferral,
    not a proof, and docs/architecture.md says so for this shape.
    """
    settings = _settings(cleanup_grace=0.4)
    deps = _deps_with(settings)
    backend = FakeBackend()
    job = make_job_row(start_to_close=timedelta(seconds=0.2))
    clock: Clock = FakeClock(_NOW_FROZEN)

    body_started = threading.Event()
    release_body = threading.Event()
    body_returned = threading.Event()
    captured_ctx: list[JobContext[EmptyPayload]] = []

    attempt = asyncio.ensure_future(
        consume_one_job(
            as_backend(backend),
            job,
            _WORKER_ID,
            deps=deps,  # type: ignore[arg-type]  # Why: MagicMock(spec=WorkerDeps) with the attrs the consumer reads set to real values.
            run_actor=_tracked_sync_run_actor(
                _gated_body(body_started, release_body, body_returned), captured_ctx
            ),
            actor_config=default_actor_config(),
            payload_type=EmptyPayload,
            clock=clock,
            settings=settings,
        )
    )
    await asyncio.to_thread(body_started.wait, 10.0)
    # timeout at ~0.2s; the 0.4s park expires with the body still gated.
    await asyncio.sleep(1.0)

    assert len(backend.mark_failed_or_retry_calls) == 1, (
        "the bounded park expires and the write lands: the row is never stranded"
    )
    assert not body_returned.is_set(), "the thread outlived the park: the write landed while it ran"
    delay = backend.mark_failed_or_retry_calls[0]["retry_delay"]
    expected_hold = timedelta(seconds=20.0 + _watchdog_exit_tail(settings))
    assert isinstance(delay, timedelta) and delay >= expected_hold, (
        "a re-pend written while the thread is provably alive must be "
        f"deferred behind the release hold ({expected_hold}); got {delay}"
    )

    release_body.set()
    with suppress(asyncio.CancelledError):
        await asyncio.wait_for(attempt, timeout=15.0)
    handle = captured_ctx[0]._sync_actor_task  # pyright: ignore[reportPrivateUsage]  # Why: joining the executor thread before the test ends.
    assert handle is not None
    done, _pending = await asyncio.wait({handle}, timeout=10.0)
    assert handle in done, "the gated body returns once released; the thread joins"


async def test_an_async_actor_timeout_keeps_the_immediate_repend() -> None:
    """The exit-proof park costs an async actor nothing.

    An async actor's timeout propagates the cancellation through its own
    frames: the actor has provably exited by the time the handler runs, no
    tracked handle is pending, and the re-pend must stay immediate, the
    park must never tax the common timeout path.
    """

    async def actor(_job: object, _ctx: JobContext[BaseModel]) -> object:
        await asyncio.sleep(30.0)
        return {"done": True}

    settings = _settings(cleanup_grace=5.0)
    deps = _deps_with(settings)
    backend = FakeBackend()
    job = make_job_row(start_to_close=timedelta(seconds=0.2))
    clock: Clock = FakeClock(_NOW_FROZEN)

    started = time.monotonic()
    outcome = await asyncio.wait_for(
        consume_one_job(
            as_backend(backend),
            job,
            _WORKER_ID,
            deps=deps,  # type: ignore[arg-type]  # Why: MagicMock(spec=WorkerDeps) with the attrs the consumer reads set to real values.
            run_actor=actor,
            actor_config=default_actor_config(),
            payload_type=EmptyPayload,
            clock=clock,
            settings=settings,
        ),
        timeout=15.0,
    )
    elapsed = time.monotonic() - started

    assert outcome in ("scheduled", "failed")
    assert len(backend.mark_failed_or_retry_calls) == 1
    assert elapsed < 2.0, (
        f"an async actor's timeout re-pend must stay immediate; took {elapsed:.2f}s"
    )


# ── The transactional timeout arm ────────────────────────────────────────


class _FakeTxConnection:
    """Minimal asyncpg.Connection stand-in with a transaction() context manager."""

    class _Transaction:
        async def __aenter__(self) -> _FakeTxConnection._Transaction:
            return self

        async def __aexit__(self, *args: object) -> None:
            pass

    def transaction(self) -> _FakeTxConnection._Transaction:
        return self._Transaction()

    async def execute(self, query: str, *args: object) -> str:
        return ""


async def test_the_transactional_timeout_arm_parks_on_the_tracked_thread_too() -> None:
    """The transactional arm's wait_for timeout routes through the same
    exit-proof hold: no re-pend while the sync thread runs.

    The TimeoutError unwinds ``_run_actor_in_tx`` (the transaction rolls
    back) and lands in the shared ``_dispatch_exception``; the handler must
    consult the same tracked handle before the retry write re-pends the row.
    """
    from taskq.worker._consumer import (
        _consume_transactional,  # pyright: ignore[reportPrivateUsage]  # Why: the second timeout arm is driven directly, the way test_consumer_coverage drives its branches.
    )

    settings = _settings(cleanup_grace=2.0)
    backend = FakeBackend()
    job = make_job_row(start_to_close=timedelta(seconds=0.2))
    enqueuer = SubJobEnqueuer(
        loop_scope_resolved={asyncpg.Connection: _FakeTxConnection()},
        worker_pool=None,
        backend=backend,
    )
    ctx = JobContext(
        job_id=job.id,
        actor=job.actor,
        queue=job.queue,
        attempt=job.attempt,
        worker_id=_WORKER_ID,
        payload=EmptyPayload(),
        jobs=enqueuer,
        log=bind_job_context(
            structlog.get_logger("taskq.test"),
            job_id=job.id,
            actor=job.actor,
            queue=job.queue,
            attempt=job.attempt,
            identity_key=None,
            trace_id="",
        ),
    )

    body_started = threading.Event()
    release_body = threading.Event()
    body_returned = threading.Event()
    captured_ctx: list[JobContext[EmptyPayload]] = []

    attempt = asyncio.ensure_future(
        _consume_transactional(
            as_backend(backend),
            job,
            _WORKER_ID,
            ctx,
            enqueuer,
            _FakeTxConnection(),
            _tracked_sync_run_actor(
                _gated_body(body_started, release_body, body_returned), captured_ctx
            ),
            default_actor_config(),
            0.2,
            timedelta(hours=24),
            None,
            trace.get_current_span(),
            structlog.get_logger("taskq.test"),
            settings=settings,
        )
    )
    await asyncio.to_thread(body_started.wait, 10.0)
    await asyncio.sleep(0.45)

    assert not body_returned.is_set(), "the gated body must still be running past the timeout"
    assert backend.mark_failed_or_retry_calls == [], (
        "the transactional arm must NOT re-pend the row while the sync "
        f"actor's thread is provably still alive; wrote {backend.mark_failed_or_retry_calls}"
    )

    release_body.set()
    with suppress(asyncio.CancelledError):
        await asyncio.wait_for(attempt, timeout=15.0)

    assert body_returned.is_set()
    assert len(backend.mark_failed_or_retry_calls) == 1
    handle = captured_ctx[0]._sync_actor_task  # pyright: ignore[reportPrivateUsage]  # Why: joining the executor thread before the test ends.
    assert handle is not None and handle.done()

"""Red-team: JobHandle.wait's timeout contract against a fetch that never returns.

The project's own rule (docs/guides/ops.md, enforced across the worker
surface): "every wait on something outside the process is bounded, and
exceeding the bound is reported." ``JobHandle.wait`` polls the backend —
a wait on something outside the process — and its docstring promises
``TimeoutError: timeout elapsed before any terminal transition was
observed`` (src/taskq/client/_handle.py:246-247).

The loop only checks the deadline BETWEEN polls::

    while True:
        row = await self._backend.get(self.job_id)   # <- unbounded await
        ...
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            raise TimeoutError()

(src/taskq/client/_handle.py:253-265). A ``backend.get`` that never
returns — a wedged pool acquire (asyncpg's default acquire timeout is
None), a black-holed connection on a backend without command_timeout, a
hung custom backend — holds the loop INSIDE the fetch while the caller's
deadline expires unenforced. ``wait(timeout=0.3)`` then blocks forever:
the documented TimeoutError never fires, violating both the docstring
and the bounded-wait rule. The client-tier PG pool carries
``dispatcher_command_timeout`` (5 s), which caps each fetch's query but
not its acquire, and the handle contract must hold for every Backend,
not just one tuned pool.

These tests pin the DESIRED observable: the deadline is enforced even
while a fetch is in flight. Every wait is bounded (``asyncio.wait`` with
a timeout) so a red never hangs the suite. In-memory tier — the defect
is in the loop shape, not in SQL.
"""

from __future__ import annotations

import asyncio
import contextlib
from datetime import UTC, datetime
from typing import Any

import pytest
from pydantic import TypeAdapter

from taskq.backend._protocol import JobRow
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.client._handle import JobHandle
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.jobs import make_enqueue_args

_NONE_ADAPTER = TypeAdapter(type(None))
_FROZEN_NOW = datetime(2025, 1, 1, tzinfo=UTC)


def _pending_row() -> JobRow:
    """A pending JobRow synthesized without an event loop.

    Uses SubJobEnqueuer._synthesize_row (the same display-only row the
    in-buffering path hands callers pre-commit) so the test needs no
    running loop and no hand-rolled 40-field literal that would drift
    from JobRow. The wait loop never reads this seed row for status —
    every status read goes through the duck-typed backend below."""
    args = make_enqueue_args(actor="rt_cs_wait_probe", queue="default")
    enqueuer = SubJobEnqueuer(
        None,  # type: ignore[arg-type]  # Why: test seam — loop_scope_resolved is unused by _synthesize_row.
        None,  # type: ignore[arg-type]  # Why: test seam — worker_pool is unused by _synthesize_row.
        InMemoryBackend(clock=FakeClock(_FROZEN_NOW)),
    )
    return enqueuer._synthesize_row(args)  # pyright: ignore[reportPrivateUsage]  # Why: deliberate test seam for loop-free row synthesis; no public equivalent exists.


class _HungFetchBackend:
    """Duck-typed Backend whose ``get`` wedges.

    ``hang_after=N`` lets the first *N* calls return the pending row
    (a healthy poll cycle) before the wedge — the production shape
    where a pool exhausts only after some successful polls. ``None``
    wedges from the first call. The handle only calls ``get`` on the
    backend it was constructed with, so this is the whole surface the
    wait loop can observe."""

    def __init__(self, row: JobRow, *, hang_after: int | None = None) -> None:
        self._row = row
        self._hang_after = hang_after
        self.calls = 0
        self._gate = asyncio.Event()  # never set — the wedged fetch

    async def get(self, job_id: Any) -> JobRow | None:  # type: ignore[override]  # Why: duck-typed Backend stand-in; signature matches Backend.get structurally.
        self.calls += 1
        if self._hang_after is not None and self.calls <= self._hang_after:
            return self._row
        await self._gate.wait()
        raise AssertionError("unreachable — the gate above never releases")


class _PromptBackend:
    """Duck-typed Backend whose ``get`` always returns the pending row."""

    def __init__(self, row: JobRow) -> None:
        self._row = row

    async def get(self, job_id: Any) -> JobRow | None:  # type: ignore[override]  # Why: duck-typed Backend stand-in matching Backend.get structurally.
        return self._row


def _handle(backend: Any) -> JobHandle[None]:  # type: ignore[type-var]  # Why: JobHandle[None] via the None adapter; R cannot be inferred through a duck-typed backend.
    return JobHandle(
        row=_pending_row(),
        result_adapter=_NONE_ADAPTER,
        was_existing=False,
        backend=backend,  # type: ignore[arg-type]  # Why: duck-typed Backend seam — the wait loop only calls .get(job_id), which the stand-ins above provide.
    )


async def test_wait_deadline_is_enforced_while_a_fetch_hangs() -> None:
    """RED contract: wait(timeout=0.3) must raise its own TimeoutError even
    when the very first backend.get never returns.

    Current behavior violates the docstring contract
    (src/taskq/client/_handle.py:246-247 "TimeoutError: timeout elapsed
    before any terminal transition was observed"): the deadline check
    lives at src/taskq/client/_handle.py:262-265, BETWEEN polls, so a
    fetch that never returns holds the loop past every deadline — the
    caller's wait blocks forever despite an explicit timeout."""
    stuck = _HungFetchBackend(_pending_row())  # every get wedges
    handle = _handle(stuck)

    task = asyncio.create_task(handle.wait(timeout=0.3))
    # Bounded observation window: well past the 0.3 s deadline, well
    # short of anything that could hang the suite.
    _done, pending = await asyncio.wait({task}, timeout=2.0)

    if task in pending:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        pytest.fail(
            "CONTRACT: handle.wait(timeout=0.3) must raise TimeoutError once the "
            "deadline elapses — the project rule is 'every wait on something "
            "outside the process is bounded, and exceeding the bound is "
            "reported'. Instead the coroutine was still parked inside "
            "backend.get() 2 s after its deadline expired: the deadline is "
            "only checked between polls (src/taskq/client/_handle.py:262-265), "
            "so a fetch that never returns defeats the timeout entirely."
        )

    exc = task.exception()
    assert isinstance(exc, TimeoutError), (
        f"expected the wait's own TimeoutError at its 0.3 s deadline, got "
        f"{type(exc).__name__ if exc is not None else 'no exception'}: the "
        "deadline must be enforced against the in-flight fetch, not only "
        "between polls"
    )


async def test_wait_deadline_is_enforced_when_a_fetch_hangs_mid_poll() -> None:
    """RED contract variant: the first two fetches return (a healthy poll
    cycle begins), the third wedges — the deadline must still fire.

    This is the production shape of the defect: a pool whose acquire
    wedges only once (exhaustion after two successful polls) converts an
    explicitly-bounded wait into an unbounded one mid-flight."""
    stuck = _HungFetchBackend(_pending_row(), hang_after=2)
    handle = _handle(stuck)

    # timeout=1.5 spans three poll cycles (interval 0.5 s): gets #1 and #2
    # return promptly at t=0 / t=0.5, get #3 wedges at t=1.0 — a full second
    # BEFORE the deadline — so the deadline can only be honored by enforcing
    # it against the in-flight fetch.
    task = asyncio.create_task(handle.wait(timeout=1.5))
    _done, pending = await asyncio.wait({task}, timeout=3.5)

    if task in pending:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        pytest.fail(
            "CONTRACT: two successful polls followed by a wedged fetch must "
            "still surface wait(timeout=1.5)'s TimeoutError; instead the "
            "coroutine was still inside backend.get() 2 s after the deadline "
            "(the fetch wedged at t=1.0, the deadline was t=1.5) — the loop's "
            "deadline check (src/taskq/client/_handle.py:262-265) cannot run "
            "until the in-flight fetch returns."
        )

    exc = task.exception()
    assert isinstance(exc, TimeoutError), (
        f"expected TimeoutError from the 0.4 s deadline, got "
        f"{type(exc).__name__ if exc is not None else 'no exception'}"
    )


async def test_wait_deadline_fires_when_fetches_return_promptly() -> None:
    """GREEN control isolating the variable: the same never-terminal job,
    a backend whose fetches DO return — the loop-level deadline fires on
    schedule. Pinned alongside the two RED tests so a fix that merely
    breaks the healthy path cannot pass unnoticed."""
    handle = _handle(_PromptBackend(_pending_row()))

    task = asyncio.create_task(handle.wait(timeout=0.2))
    _done, pending = await asyncio.wait({task}, timeout=3.0)
    if task in pending:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        pytest.fail("control: even the healthy poll path failed to time out within 3 s")

    exc = task.exception()
    assert isinstance(exc, TimeoutError), (
        "control: wait(timeout=0.2) on a never-terminal job with prompt "
        f"fetches must raise TimeoutError, got "
        f"{type(exc).__name__ if exc is not None else 'no exception'}"
    )

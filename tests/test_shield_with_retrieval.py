"""Tests for taskq._shield.shield_with_retrieval.

The helper must be byte-for-byte asyncio.shield on the non-cancelled path
(result returned, inner exception re-raised to the caller) and must, when
a cancellation detaches the inner task, retrieve that detached outcome —
logging an inner failure instead of leaving asyncio to report
"Task exception was never retrieved" (the lost-infra-signal bug measured
during the reconnect-storm campaign: 2 unretrieved exceptions per
double-cancel).
"""

import asyncio
import contextlib
import gc
from collections.abc import Awaitable
from types import SimpleNamespace

import pytest
import structlog

from taskq._ids import new_uuid
from taskq._shield import shield_with_retrieval
from taskq.backend._protocol import JobId
from taskq.worker.cancel import ActiveJobRegistry, _CancelController


def _install_capture_handler() -> tuple[list[dict[str, object]], object]:
    """Install a loop exception handler capturing un-retrieved-exception
    reports; return (captured contexts, previous handler to restore).
    """
    calls: list[dict[str, object]] = []
    loop = asyncio.get_running_loop()

    def handler(loop: object, context: dict[str, object]) -> None:
        calls.append(context)

    prev = loop.set_exception_handler(handler)
    return calls, prev


async def _drive_site(inner: Awaitable[object]) -> None:
    """Simulate a consumer terminal-write site: the first cancel is already
    being handled; the shielded write below eats the second one.
    """
    try:
        await asyncio.sleep(3600)
    except asyncio.CancelledError:
        try:
            await shield_with_retrieval(inner)
        finally:
            raise


class TestNonCancelledPath:
    async def test_result_is_returned(self) -> None:
        """Inner success: the result is awaited normally."""

        async def inner() -> int:
            await asyncio.sleep(0)
            return 42

        assert await shield_with_retrieval(inner()) == 42

    async def test_inner_exception_reraises_to_caller(self) -> None:
        """Inner failure with no cancellation: shield semantics — the
        exception propagates to the awaiting caller, unchanged.
        """

        async def inner() -> None:
            raise ValueError("boom")

        with pytest.raises(ValueError, match="boom"):
            await shield_with_retrieval(inner())

    async def test_inner_failure_on_normal_path_is_not_detached_logged(self) -> None:
        """When the caller receives the inner exception directly, no
        detached-retrieval warning is logged (the callback only attaches on
        the cancellation path — no duplicate reporting).
        """

        async def inner() -> None:
            raise ValueError("boom")

        with structlog.testing.capture_logs() as logs:
            with contextlib.suppress(ValueError):
                await shield_with_retrieval(inner())
            await asyncio.sleep(0)

        assert not any(e["event"] == "shield-detached-task-failed" for e in logs)


class TestCancellationPath:
    async def test_outer_cancel_raises_and_inner_keeps_running(self) -> None:
        """First cancel during the shielded await: the caller sees
        CancelledError and the inner coroutine keeps running to completion
        (plain shield semantics preserved).
        """
        inner_finished = False
        release = asyncio.Event()

        async def inner() -> None:
            nonlocal inner_finished
            await release.wait()
            inner_finished = True

        async def site() -> None:
            await shield_with_retrieval(inner())

        task = asyncio.create_task(site())
        await asyncio.sleep(0)
        task.cancel("first cancel")
        with contextlib.suppress(asyncio.CancelledError):
            await task

        assert not inner_finished
        release.set()
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert inner_finished

    async def test_double_cancel_inner_failure_is_logged_not_lost(self) -> None:
        """The campaign scenario: a second CancelledError lands while the
        detached inner write is still running, and the inner write FAILS.

        The failure must be logged (shield-detached-task-failed) and must
        NOT surface as asyncio's "Task exception was never retrieved".
        """
        started = asyncio.Event()
        release = asyncio.Event()
        ran_detached = False

        async def inner_write() -> None:
            nonlocal ran_detached
            started.set()
            await release.wait()
            ran_detached = True
            raise RuntimeError("pg went down mid-write")

        calls, prev_handler = _install_capture_handler()
        try:
            with structlog.testing.capture_logs() as logs:
                sim = asyncio.create_task(_drive_site(inner_write()))
                await asyncio.sleep(0)
                sim.cancel("first cancel")
                # Cancel #1 lands on the actor-body sleep; the handler then
                # enters the shielded write, which starts the inner task.
                await started.wait()
                await asyncio.sleep(0)
                sim.cancel("second cancel")
                with contextlib.suppress(asyncio.CancelledError):
                    await sim

                # Detached inner still running here. Let it fail, then give
                # the done-callback a chance to run and retrieve it.
                release.set()
                for _ in range(5):
                    await asyncio.sleep(0)

                assert ran_detached
                detached = [e for e in logs if e["event"] == "shield-detached-task-failed"]
                assert len(detached) == 1
                assert detached[0]["error_type"] == "RuntimeError"
                assert "pg went down mid-write" in str(detached[0]["error"])

            # Nothing was left for the GC to report as unretrieved.
            gc.collect()
            await asyncio.sleep(0)
            assert not any("never retrieved" in str(c.get("message", "")) for c in calls), (
                f"unretrieved-task report reached the loop handler: {calls}"
            )
        finally:
            asyncio.get_running_loop().set_exception_handler(prev_handler)

    async def test_double_cancel_inner_success_is_silent(self) -> None:
        """Detached inner that eventually SUCCEEDS: no warning (success is
        superseded by the cancellation handling, nothing to retrieve).
        """
        started = asyncio.Event()
        release = asyncio.Event()

        async def inner_write() -> None:
            started.set()
            await release.wait()

        with structlog.testing.capture_logs() as logs:
            sim = asyncio.create_task(_drive_site(inner_write()))
            await asyncio.sleep(0)
            sim.cancel("first cancel")
            await started.wait()
            await asyncio.sleep(0)
            sim.cancel("second cancel")
            with contextlib.suppress(asyncio.CancelledError):
                await sim
            release.set()
            for _ in range(5):
                await asyncio.sleep(0)

        assert not any(e["event"] == "shield-detached-task-failed" for e in logs)


class TestCancelAbandonSite:
    """The ``run_post_tx`` abandon site (``taskq.worker.cancel``).

    ``mark_abandoned`` ran under plain ``asyncio.shield`` — the one shield
    site the reconnect-storm campaign's conversion pass skipped — so a
    double cancel (shutdown racing the force-cancel escalation) detached a
    failing PG write that nobody retrieved. The test drives the REAL
    controller: a first cancel (heartbeat tick body) hands control to
    ``run_post_tx`` via its always-run contract; the second cancel lands on
    the shielded abandon write itself.
    """

    async def test_double_cancel_abandon_failure_is_logged_not_lost(self) -> None:
        started = asyncio.Event()
        release = asyncio.Event()
        ran_detached = False

        class _FailingBackend:
            async def mark_abandoned(self, job_id: JobId) -> bool:
                nonlocal ran_detached
                started.set()
                await release.wait()
                ran_detached = True
                raise RuntimeError("pg went down mid-abandon")

        deps = SimpleNamespace(
            settings=SimpleNamespace(
                schema_name="jobs",
                cancellation_grace_period=0.0,
                cleanup_grace_period=0.0,
            ),
            active_jobs=ActiveJobRegistry(),
        )
        controller = _CancelController(
            deps,  # pyright: ignore[reportArgumentType]  # Why: run_post_tx only touches settings' grace fields and active_jobs, all stubbed above
            new_uuid(),
            _FailingBackend(),  # pyright: ignore[reportArgumentType]  # Why: only mark_abandoned is called on the abandon path
        )
        controller._pending_abandons.append(JobId(new_uuid()))  # pyright: ignore[reportPrivateUsage]  # Why: run_in_tx queues abandons from a PG tick; the unit test seeds the queue directly

        async def heartbeat_finally_site() -> None:
            try:
                await asyncio.sleep(3600)  # the tick body
            finally:
                # heartbeat_loop's contract: run_post_tx even on cancellation.
                await controller.run_post_tx()

        calls, prev_handler = _install_capture_handler()
        try:
            with structlog.testing.capture_logs() as logs:
                site = asyncio.create_task(heartbeat_finally_site())
                await asyncio.sleep(0)
                site.cancel("shutdown cancel")
                # Cancel #1 lands on the tick-body sleep; the finally enters
                # run_post_tx, which starts the abandon write.
                await started.wait()
                await asyncio.sleep(0)
                site.cancel("second cancel")
                with contextlib.suppress(asyncio.CancelledError):
                    await site

                # Detached abandon write still running. Let it fail, then
                # give the done-callback a chance to retrieve it.
                release.set()
                for _ in range(5):
                    await asyncio.sleep(0)

                assert ran_detached
                detached = [e for e in logs if e["event"] == "shield-detached-task-failed"]
                assert len(detached) == 1
                assert detached[0]["error_type"] == "RuntimeError"
                assert "pg went down mid-abandon" in str(detached[0]["error"])

            gc.collect()
            await asyncio.sleep(0)
            assert not any("never retrieved" in str(c.get("message", "")) for c in calls), (
                f"unretrieved-task report reached the loop handler: {calls}"
            )
        finally:
            asyncio.get_running_loop().set_exception_handler(prev_handler)

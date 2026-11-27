"""Generation-based reload of a workflow app's ``deps_factory`` (#719).

A long-running worker (or a long-lived :class:`FlowRunner`) hands bodies ONE
factory-created instance. When the factory's inputs change — a settings
reload, a rotated downstream client — that instance is stale: it must be
replaced, and the replacement must never tear the old resource down under a
body that is still using it.

These pins drive the real machinery:

* the worker-and-loop scope's rotation law — a generation advance swaps the
  instance, drains the superseded generation's in-flight bodies (bounded, with
  timeout telemetry), releases it exactly once, and never turns a claim that
  was mid-resolution into the ``deps owner is closed`` failure;
* the direct door's explicit trigger (``FlowRunner.reload_deps``) against the
  real PG fixtures, including a reload landing while a body is executing;
* the worker door's hand-off (``reload_worker_workflow_state`` →
  ``rotate_worker_deps``) through the real fleet-claimed execution door, with
  the runner cache rebuilt and a co-resident worker untouched;
* the reload ordering contract inside ``reload_credentials``: the pre-swap
  drain runs BEFORE any old pool closes (driven through the real
  ``open_worker_deps`` with the suite's fake-pool convention), and a failing
  drain is reported without aborting the rotation;
* the real worker process (``_bootstrap._main``): a rolling reload while a
  slow body is still running, and a body that outlives the drain bound.

Deterministic coordination throughout: events the production code itself
signals (the generation source read, a body starting, a lease leaving) — no
wall-clock sleeps in the ordering assertions.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import AsyncGenerator, Awaitable, Callable, Iterator
from datetime import UTC, datetime, timedelta
from typing import Any, Self, cast
from uuid import UUID

import asyncpg
import pytest
import structlog.testing
from pydantic import BaseModel

import taskq.worker._bootstrap as bootstrap_mod
import taskq.worker.deps as worker_deps_mod
from taskq._ids import new_uuid
from taskq.backend._protocol import JobId, JobRow
from taskq.connections import WorkerConnections
from taskq.settings import WorkerSettings
from taskq.testing.assertions import wait_for, wait_for_condition
from taskq.testing.fixtures import ModulePgSchema
from taskq.testing.health import unique_health_sock_path
from taskq.worker.deps import open_worker_deps
from taskq.workflows import ChildOk, FlowRunner, Promise, StepContext, WorkflowApp, build, step
from taskq.workflows import _worker_deps as scope
from taskq.workflows import _worker_execution as seam
from taskq.workflows.api._deps_factory import DepsFactory

# ── the probe resource, its generation, and the observing bodies ─────────

_GENERATION: list[int] = [0]
"""The factory's declared generation — what a real declaration's source reads
out of settings. Bumped by the reload pins, reset per test."""

_PRODUCED: list[int] = [0]
"""Factory invocation count (one mint per generation)."""

_INSTANCES: list[object] = []
"""The deps instance each body observed, in execution order."""

_EXECUTED: list[str] = []
"""One entry per body that started (the body's declared workflow name)."""

_ORDER: list[str] = []
"""Lifecycle markers in the order they happened: body boundaries, the
generation source read, each deps release, each role pool's bounded close."""

_RELEASES: list[int] = []
"""The generation of every released instance (the exact-once witness)."""


class _Signals:
    """The probe's loop-bound synchronisation primitives.

    Recreated per test: each scope pin drives its own ``asyncio.run`` loop, and
    an ``asyncio.Event`` binds to the loop that first waits on it.
    """

    def __init__(self) -> None:
        self.generation_read = asyncio.Event()
        self.started = asyncio.Event()


_SIGNALS: list[_Signals] = [_Signals()]

_HOLD: list[asyncio.Event] = []
"""Holds the gate the FIRST body of a test waits on (the reload's in-flight
body), when the test installed one."""

_FAILURES: list[int] = [0]
"""The flaky factory's remaining forced failures."""


def _reset() -> None:
    _GENERATION[0] = 0
    _PRODUCED[0] = 0
    _INSTANCES.clear()
    _EXECUTED.clear()
    _ORDER.clear()
    _RELEASES.clear()
    _FAILURES[0] = 0
    _HOLD.clear()
    _SIGNALS[0] = _Signals()


@pytest.fixture(autouse=True)
def _reset_probe_state() -> Iterator[None]:  # pyright: ignore[reportUnusedFunction]  # Why: autouse fixture consumed implicitly by the test runner.
    _reset()
    yield


async def _resolved(resolution: scope.WorkerDepsResolution) -> object:
    """The instance *resolution* serves, read through the same face the runner
    drives (the body lease) — the pins never exercise a seam production does
    not."""
    async with resolution.body() as instance:
        return instance


class _Probe:
    """The factory-created deps resource: loop-bound, released by ``aclose()``
    (the shape a pool or an HTTP client has), stamped with the generation that
    minted it."""

    def __init__(self, loop: asyncio.AbstractEventLoop, generation: int) -> None:
        self.loop = loop
        self.generation = generation
        self.closed = False

    async def aclose(self) -> None:
        self.closed = True
        _ORDER.append(f"deps-teardown:{self.generation}")
        _RELEASES.append(self.generation)


async def _make_probe() -> _Probe:
    _PRODUCED[0] += 1
    return _Probe(asyncio.get_running_loop(), _GENERATION[0])


async def _make_flaky_probe() -> _Probe:
    _PRODUCED[0] += 1
    if _FAILURES[0] > 0:
        _FAILURES[0] -= 1
        try:
            raise OSError("deps backend unreachable")
        except OSError as exc:
            raise RuntimeError("replacement deps factory exploded") from exc
    return _Probe(asyncio.get_running_loop(), _GENERATION[0])


def _declared_generation() -> int:
    """The production generation source: read by the rotation, and the signal
    the tests synchronise on (no sleep races)."""
    _ORDER.append("generation-read")
    _SIGNALS[0].generation_read.set()
    return _GENERATION[0]


def _failing_generation() -> int:
    raise RuntimeError("settings snapshot unavailable")


class _Doc(BaseModel):
    doc_id: str


class _Report(BaseModel):
    ref: str


async def _deps_body(ctx: StepContext, doc: _Doc, deps: _Probe) -> _Report:
    assert deps.loop is asyncio.get_running_loop(), "the factory did not run on this loop"
    _EXECUTED.append(doc.doc_id)
    _INSTANCES.append(deps)
    _ORDER.append(f"body-start:{doc.doc_id}")
    _SIGNALS[0].started.set()
    if _HOLD and _EXECUTED.count(doc.doc_id) == 1:
        await _HOLD[0].wait()
    _ORDER.append(f"body-end:{doc.doc_id}")
    return _Report(ref=doc.doc_id)


def _reload_app(
    names: list[str],
    *,
    generation: Callable[[], int] | None = _declared_generation,
    factory: Callable[[], Awaitable[_Probe]] = _make_probe,
) -> WorkflowApp:
    """One app over the probe factory; each name is one workflow whose body
    declares the deps parameter (and whose declared doc id is its own name)."""
    app = WorkflowApp(deps_factory=factory, deps_generation=generation)
    for name in names:

        @app.workflow(name)
        def _build(name: str = name) -> Promise[object]:
            return build(
                step(
                    _deps_body,
                    _Doc(doc_id=name),
                    key="solo",
                    actor="wf-reload",
                    queue="default",
                )
            )

    return app


# ── the generation rotation law (the scope, no PG) ───────────────────────


def test_a_generation_source_requires_an_explicit_factory_at_both_public_doors() -> None:
    """A generation rotates only the factory declared at that door: neither
    a static app nor a runner-side generation-only override is meaningful."""
    for deps in (None, object()):
        with pytest.raises(ValueError, match="deps_generation= requires deps_factory="):
            WorkflowApp(deps=deps, deps_generation=lambda: 1)

    app = _reload_app(["wf-generation-declaration"])
    with pytest.raises(ValueError, match="deps_generation= requires deps_factory="):
        FlowRunner(
            app.get("wf-generation-declaration"),
            cast("Any", object()),
            "unused_schema",
            deps_generation=lambda: 1,
        )


def test_a_generation_advance_swaps_the_instance_and_releases_the_old_one_once() -> None:
    """The declared generation advanced → the next claim mints the new
    generation's instance, the superseded one is released exactly once with its
    own generation reported, and the transition is fully observable."""
    app = _reload_app(["wf-rotate"])
    compiled = app.get("wf-rotate")
    binding = compiled.deps_factory
    assert binding is not None
    worker_id = new_uuid()

    async def _scenario() -> None:
        resolution = scope.prepare_worker_deps(binding, workflow="wf-rotate", worker_id=worker_id)
        with structlog.testing.capture_logs() as logs:
            first = cast("_Probe", await _resolved(resolution))
            assert (first.generation, _PRODUCED[0]) == (0, 1)

            _GENERATION[0] = 1
            report = await resolution.rotate(drain_timeout=1.0)
            second = cast("_Probe", await _resolved(resolution))

        assert report.rotated and report.drained
        assert (report.previous_generation, report.generation) == (0, 1)
        assert second is not first
        assert second.generation == 1
        assert _PRODUCED[0] == 2
        assert _RELEASES == [0], f"the superseded instance released once; got {_RELEASES}"
        assert resolution.generation == 1

        torn = [e for e in logs if e["event"] == "workflow-deps-torn-down"]
        assert len(torn) == 1, torn
        assert (torn[0]["workflow"], torn[0]["worker_id"], torn[0]["generation"]) == (
            "wf-rotate",
            str(worker_id),
            0,
        )
        resolved = [e for e in logs if e["event"] == "workflow-deps-resolved"]
        assert [e["generation"] for e in resolved] == [0, 1], resolved
        rotated = next(e for e in logs if e["event"] == "workflow-deps-rotated")
        assert (rotated["previous_generation"], rotated["generation"]) == (0, 1)
        assert rotated["worker_id"] == str(worker_id)

        await scope.teardown_worker_deps(worker_id)
        assert _RELEASES == [0, 1], "the shutdown release names the live generation"

    asyncio.run(_scenario())


def test_an_unchanged_generation_rotates_nothing() -> None:
    """A reload that finds the declared generation unchanged leaves the
    instance, the factory and the telemetry untouched."""
    binding = DepsFactory(_make_probe, generation=lambda: 7)
    worker_id = new_uuid()

    async def _scenario() -> None:
        resolution = scope.prepare_worker_deps(binding, workflow="wf-same", worker_id=worker_id)
        await _resolved(resolution)
        first = await resolution.rotate(drain_timeout=1.0)
        assert first.rotated, "the first reload adopts the declared generation"
        assert (first.previous_generation, first.generation) == (0, 7)
        instance = await _resolved(resolution)
        with structlog.testing.capture_logs() as logs:
            report = await resolution.rotate(drain_timeout=1.0)
        assert not report.rotated
        assert (report.previous_generation, report.generation) == (7, 7)
        assert _RELEASES == [0], "only the superseding rotation released"
        assert await _resolved(resolution) is instance, "the same instance keeps serving"
        assert [e for e in logs if e["event"].startswith("workflow-deps-")] == []

    asyncio.run(_scenario())


def test_a_rotation_waits_for_the_bodies_holding_the_superseded_instance() -> None:
    """A body already executing on the old generation finishes against it; the
    release happens only after it leaves, and a body claiming meanwhile gets
    the NEW generation's instance without waiting for the old body."""
    binding = DepsFactory(_make_probe, generation=_declared_generation)
    worker_id = new_uuid()

    async def _scenario() -> None:
        resolution = scope.prepare_worker_deps(binding, workflow="wf-drain", worker_id=worker_id)
        release = asyncio.Event()

        async def _running_body() -> None:
            async with resolution.body(workflow="wf-drain"):
                await release.wait()

        body = asyncio.create_task(_running_body())
        # the running body holds generation 0's instance
        async with resolution.body(workflow="wf-drain") as warm:
            assert cast("_Probe", warm).generation == 0
        assert _RELEASES == [], "a lease that ended does not release anything by itself"

        _GENERATION[0] = 1
        rotation = asyncio.create_task(resolution.rotate(drain_timeout=30.0))
        await asyncio.wait_for(_SIGNALS[0].generation_read.wait(), timeout=5.0)

        assert not rotation.done(), "the rotation must not finish while a body holds the instance"
        assert _RELEASES == [], "the superseded instance is not released under a live body"

        async with resolution.body(workflow="wf-drain") as fresh:
            assert cast("_Probe", fresh).generation == 1
            assert _RELEASES == []

        release.set()
        report = await asyncio.wait_for(rotation, timeout=30.0)
        await body

        assert report.rotated and report.drained
        assert _RELEASES == [0], f"released once, after the last old body left; got {_RELEASES}"
        await scope.teardown_worker_deps(worker_id)
        assert _RELEASES == [0, 1]

    asyncio.run(_scenario())


def test_a_body_outliving_the_drain_bound_is_released_once_with_timeout_telemetry() -> None:
    """The drain bound ends clean-completion protection: the superseded
    instance is released anyway (old credentials cannot stay open forever),
    the timeout is reported with ``workflow``, ``worker_id`` and
    ``generation``, and the stuck body's later exit releases nothing more."""
    binding = DepsFactory(_make_probe, generation=_declared_generation)
    worker_id = new_uuid()

    async def _scenario() -> None:
        resolution = scope.prepare_worker_deps(binding, workflow="wf-stuck", worker_id=worker_id)
        release = asyncio.Event()
        entered = asyncio.Event()

        async def _stuck_body() -> None:
            async with resolution.body(workflow="wf-stuck"):
                entered.set()
                await release.wait()

        body = asyncio.create_task(_stuck_body())
        await asyncio.wait_for(entered.wait(), timeout=5.0)

        _GENERATION[0] = 1
        with structlog.testing.capture_logs() as logs:
            report = await resolution.rotate(drain_timeout=0.05)

        assert report.rotated
        assert not report.drained, "the drain bound expired while the body was still running"
        assert _RELEASES == [0], "the bounded release still ran, exactly once"
        timeout = next(e for e in logs if e["event"] == "workflow-deps-drain-timeout")
        assert (timeout["workflow"], timeout["worker_id"], timeout["generation"]) == (
            "wf-stuck",
            str(worker_id),
            0,
        )
        assert timeout["drain_timeout"] == 0.05
        assert [e["event"] for e in logs if e["event"] == "workflow-deps-torn-down"] == [
            "workflow-deps-torn-down"
        ]

        release.set()
        await body
        assert _RELEASES == [0], "the stuck body's unwind must not release a second time"

        await scope.teardown_worker_deps(worker_id)
        assert _RELEASES == [0], "nothing was resolved on the new generation, so nothing releases"

    asyncio.run(_scenario())


def test_a_failed_superseded_release_is_reported_and_never_retried() -> None:
    """A teardown failure belongs to the generation it released: the
    rotation reports the cause, telemetry names that generation, the successor
    still resolves, and shutdown does not retry the failed attempt."""

    async def _teardown(instance: object) -> None:
        generation = cast("_Probe", instance).generation
        _RELEASES.append(generation)
        if generation == 0:
            raise RuntimeError("old credential client close exploded")

    binding = DepsFactory(_make_probe, teardown=_teardown, generation=_declared_generation)
    worker_id = new_uuid()

    async def _scenario() -> None:
        resolution = scope.prepare_worker_deps(
            binding, workflow="wf-release-error", worker_id=worker_id
        )
        await _resolved(resolution)
        _GENERATION[0] = 1
        with structlog.testing.capture_logs() as logs:
            report = await resolution.rotate(drain_timeout=1.0)

        assert report.rotated and isinstance(report.error, RuntimeError)
        failure = next(entry for entry in logs if entry["event"] == "workflow-deps-teardown-error")
        assert (failure["workflow"], failure["worker_id"], failure["generation"]) == (
            "wf-release-error",
            str(worker_id),
            0,
        )
        assert isinstance(failure["exc_info"], RuntimeError)

        await _resolved(resolution)
        await scope.teardown_worker_deps(worker_id)
        assert _RELEASES == [0, 1], "the failed old release is not retried at shutdown"

    asyncio.run(_scenario())


def test_a_superseded_release_timeout_is_bounded_and_reported(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The reload's close bound is independent of its drain bound: a stuck
    release is cancelled once, reported against the old generation, and does
    not wedge either the successor or shutdown."""
    monkeypatch.setattr(scope, "_TEARDOWN_TIMEOUT_SECS", 0.05)
    never = asyncio.Event()

    async def _teardown(instance: object) -> None:
        generation = cast("_Probe", instance).generation
        _RELEASES.append(generation)
        if generation == 0:
            await never.wait()

    binding = DepsFactory(_make_probe, teardown=_teardown, generation=_declared_generation)
    worker_id = new_uuid()

    async def _scenario() -> None:
        resolution = scope.prepare_worker_deps(
            binding, workflow="wf-release-timeout", worker_id=worker_id
        )
        await _resolved(resolution)
        _GENERATION[0] = 1
        with structlog.testing.capture_logs() as logs:
            report = await resolution.rotate(drain_timeout=1.0)

        assert report.rotated and isinstance(report.error, TimeoutError)
        timeout = next(
            entry for entry in logs if entry["event"] == "workflow-deps-teardown-timeout"
        )
        assert (timeout["workflow"], timeout["worker_id"], timeout["generation"]) == (
            "wf-release-timeout",
            str(worker_id),
            0,
        )
        assert timeout["close_timeout"] == 0.05

        await _resolved(resolution)
        await scope.teardown_worker_deps(worker_id)
        assert _RELEASES == [0, 1], "the timed-out old release is not retried at shutdown"

    asyncio.run(_scenario())


def test_a_cancelled_rotation_leaves_the_superseded_instance_to_shutdown() -> None:
    """A shutdown that cancels a rotation mid-drain (a SIGTERM inside the drain
    window) abandones nothing: the superseded instance is released by the
    worker's shutdown teardown — once, before the role pools close — and the
    teardown names the generation it released."""
    binding = DepsFactory(_make_probe, generation=_declared_generation)
    worker_id = new_uuid()

    async def _scenario() -> None:
        resolution = scope.prepare_worker_deps(binding, workflow="wf-cancel", worker_id=worker_id)
        release = asyncio.Event()
        entered = asyncio.Event()

        async def _running_body() -> None:
            async with resolution.body(workflow="wf-cancel"):
                entered.set()
                await release.wait()

        body = asyncio.create_task(_running_body())
        await asyncio.wait_for(entered.wait(), timeout=5.0)

        _GENERATION[0] = 1
        rotation = asyncio.create_task(resolution.rotate(drain_timeout=30.0))
        await asyncio.wait_for(_SIGNALS[0].generation_read.wait(), timeout=5.0)
        rotation.cancel()
        with pytest.raises(asyncio.CancelledError):
            await rotation
        assert _RELEASES == [], "the cancelled rotation released nothing itself"

        release.set()
        await body

        with structlog.testing.capture_logs() as logs:
            await scope.teardown_worker_deps(worker_id)

        assert _RELEASES == [0], (
            f"shutdown released the superseded generation's instance exactly once; got {_RELEASES}"
        )
        torn = [e for e in logs if e["event"] == "workflow-deps-torn-down"]
        assert [e["generation"] for e in torn] == [0], torn
        await scope.teardown_worker_deps(worker_id)
        assert _RELEASES == [0], "a second shutdown releases nothing more"

    asyncio.run(_scenario())


def test_shutdown_reports_each_pending_generations_own_teardown_outcome() -> None:
    """A cancelled rotation can leave both the superseded and current
    generations resolved at shutdown. A failing old release must not be
    reported against the current generation or turn its successful close into
    a failure."""

    async def _teardown(instance: object) -> None:
        generation = cast("_Probe", instance).generation
        _RELEASES.append(generation)
        if generation == 0:
            raise RuntimeError("generation zero close failed")

    binding = DepsFactory(_make_probe, teardown=_teardown, generation=_declared_generation)
    worker_id = new_uuid()

    async def _scenario() -> None:
        resolution = scope.prepare_worker_deps(
            binding, workflow="wf-pending-outcomes", worker_id=worker_id
        )
        await _resolved(resolution)
        release = asyncio.Event()
        entered = asyncio.Event()

        async def _running_body() -> None:
            async with resolution.body(workflow="wf-pending-outcomes"):
                entered.set()
                await release.wait()

        body = asyncio.create_task(_running_body())
        await asyncio.wait_for(entered.wait(), timeout=5.0)
        _GENERATION[0] = 1
        rotation = asyncio.create_task(resolution.rotate(drain_timeout=30.0))
        await asyncio.wait_for(_SIGNALS[0].generation_read.wait(), timeout=5.0)
        rotation.cancel()
        with pytest.raises(asyncio.CancelledError):
            await rotation
        release.set()
        await body
        await _resolved(resolution)

        with structlog.testing.capture_logs() as logs:
            await scope.teardown_worker_deps(worker_id)

        assert sorted(_RELEASES) == [0, 1]
        failure = next(entry for entry in logs if entry["event"] == "workflow-deps-teardown-error")
        assert failure["generation"] == 0, failure
        successes = [entry for entry in logs if entry["event"] == "workflow-deps-torn-down"]
        assert [entry["generation"] for entry in successes] == [1], successes

    asyncio.run(_scenario())


def test_shutdown_overlapping_an_active_rotation_releases_once_without_a_false_failure() -> None:
    """Shutdown may begin while a reload is draining. Both owners may await
    the same superseded entry, but the rotation must still report the rotation
    it already committed and shutdown must complete its one release."""
    binding = DepsFactory(_make_probe, generation=_declared_generation)
    worker_id = new_uuid()

    async def _scenario() -> None:
        resolution = scope.prepare_worker_deps(binding, workflow="wf-overlap", worker_id=worker_id)
        release = asyncio.Event()
        entered = asyncio.Event()

        async def _running_body() -> None:
            async with resolution.body(workflow="wf-overlap"):
                entered.set()
                await release.wait()

        body = asyncio.create_task(_running_body())
        await asyncio.wait_for(entered.wait(), timeout=5.0)
        _GENERATION[0] = 1
        with structlog.testing.capture_logs() as logs:
            rotation = asyncio.create_task(resolution.rotate(drain_timeout=30.0))
            await asyncio.wait_for(_SIGNALS[0].generation_read.wait(), timeout=5.0)

            shutdown = asyncio.create_task(scope.teardown_worker_deps(worker_id))
            release.set()
            await body
            report = await asyncio.wait_for(rotation, timeout=5.0)
            await asyncio.wait_for(shutdown, timeout=5.0)

        assert report.rotated, f"the committed rotation was misreported: {report}"
        assert report.error is None, f"the overlapping close became a false failure: {report.error}"
        assert _RELEASES == [0], f"the superseded instance released exactly once: {_RELEASES}"
        torn = [entry for entry in logs if entry["event"] == "workflow-deps-torn-down"]
        assert len(torn) == 1, f"one physical release has one lifecycle report: {torn}"

    asyncio.run(_scenario())


def test_shutdown_preserves_a_pending_generation_report_when_rotation_wins_its_close() -> None:
    """Shutdown closes the current generation first. If the overlapping
    rotation releases the pending generation meanwhile, shutdown's later
    idempotent close must not erase that generation's lifecycle report."""
    current_close_started = asyncio.Event()
    finish_current_close = asyncio.Event()

    async def _teardown(instance: object) -> None:
        generation = cast("_Probe", instance).generation
        if generation == 1:
            current_close_started.set()
            await finish_current_close.wait()
        _RELEASES.append(generation)

    binding = DepsFactory(_make_probe, teardown=_teardown, generation=_declared_generation)
    worker_id = new_uuid()

    async def _scenario() -> None:
        resolution = scope.prepare_worker_deps(
            binding, workflow="wf-overlap-report", worker_id=worker_id
        )
        await _resolved(resolution)
        release_body = asyncio.Event()
        body_started = asyncio.Event()

        async def _running_body() -> None:
            async with resolution.body(workflow="wf-overlap-report"):
                body_started.set()
                await release_body.wait()

        body = asyncio.create_task(_running_body())
        await asyncio.wait_for(body_started.wait(), timeout=5.0)
        _GENERATION[0] = 1
        rotation = asyncio.create_task(resolution.rotate(drain_timeout=30.0))
        await asyncio.wait_for(_SIGNALS[0].generation_read.wait(), timeout=5.0)
        await _resolved(resolution)  # the current generation is live at shutdown too

        with structlog.testing.capture_logs() as logs:
            shutdown = asyncio.create_task(scope.teardown_worker_deps(worker_id))
            await asyncio.wait_for(current_close_started.wait(), timeout=5.0)
            release_body.set()
            await body
            report = await asyncio.wait_for(rotation, timeout=5.0)
            assert report.rotated
            finish_current_close.set()
            await asyncio.wait_for(shutdown, timeout=5.0)

        assert sorted(_RELEASES) == [0, 1]
        torn = [entry for entry in logs if entry["event"] == "workflow-deps-torn-down"]
        assert sorted(entry["generation"] for entry in torn) == [0, 1], torn

    asyncio.run(_scenario())


def test_every_rotated_instance_is_released_exactly_once_across_rotations_and_shutdown() -> None:
    """Three generations on one worker, then shutdown: exactly one release per
    resolved instance — the totals must match."""
    binding = DepsFactory(_make_probe, generation=_declared_generation)
    worker_id = new_uuid()

    async def _scenario() -> None:
        resolution = scope.prepare_worker_deps(binding, workflow="wf-many", worker_id=worker_id)
        await _resolved(resolution)
        for generation in (1, 2):
            _GENERATION[0] = generation
            report = await resolution.rotate(drain_timeout=1.0)
            assert report.rotated
            await _resolved(resolution)
            # a second reload naming the same generation is a no-op
            assert not (await resolution.rotate(drain_timeout=1.0)).rotated

        await scope.teardown_worker_deps(worker_id)
        await scope.teardown_worker_deps(worker_id)
        assert _PRODUCED[0] == 3
        assert _RELEASES == [0, 1, 2], f"one release per instance; got {_RELEASES}"

    asyncio.run(_scenario())


def test_a_claim_mid_resolution_across_a_rotation_gets_the_new_generation() -> None:
    """A reload must never turn a legitimate claim into ``deps owner is
    closed``: a claim parked inside the old generation's factory completes on
    the new generation, and the late old-generation resource is released rather
    than handed out."""
    permit_first = asyncio.Event()

    async def _gated_probe() -> _Probe:
        _PRODUCED[0] += 1
        if _PRODUCED[0] == 1:
            await permit_first.wait()
        return _Probe(asyncio.get_running_loop(), _GENERATION[0])

    binding = DepsFactory(_gated_probe, generation=_declared_generation)
    worker_id = new_uuid()

    async def _scenario() -> None:
        resolution = scope.prepare_worker_deps(binding, workflow="wf-race", worker_id=worker_id)
        claim = asyncio.create_task(_resolved(resolution))
        await wait_for_condition(
            lambda: _PRODUCED[0] == 1,
            description="the first claim entered the factory",
            timeout=5.0,
        )

        _GENERATION[0] = 1
        report = await resolution.rotate(drain_timeout=0.05)
        assert report.rotated
        assert report.error is not None, "the parked acquisition held the old lock past the bound"

        permit_first.set()
        instance = cast("_Probe", await asyncio.wait_for(claim, timeout=5.0))
        assert instance.generation == 1, "the claim completed on the new generation"
        # the late old-generation resource was released, never handed to a body
        assert len(_RELEASES) == 1, f"the late acquisition is released; got {_RELEASES}"
        assert _PRODUCED[0] == 2

        await scope.teardown_worker_deps(worker_id)
        assert len(_RELEASES) == 2, f"the live generation released at shutdown; got {_RELEASES}"

    asyncio.run(_scenario())


def test_a_failing_replacement_factory_is_reported_and_recovers() -> None:
    """The new generation's factory raising surfaces with its cause (the
    attempt's verdict is the body's), caches nothing, and the next claim
    resolves the generation cleanly."""
    _FAILURES[0] = 0
    binding = DepsFactory(_make_flaky_probe, generation=_declared_generation)
    worker_id = new_uuid()

    async def _scenario() -> None:
        resolution = scope.prepare_worker_deps(binding, workflow="wf-flaky", worker_id=worker_id)
        await _resolved(resolution)
        _GENERATION[0] = 1
        await resolution.rotate(drain_timeout=1.0)
        assert _RELEASES == [0]
        _FAILURES[0] = 1  # the replacement mint fails once

        with (
            structlog.testing.capture_logs() as logs,
            pytest.raises(RuntimeError, match="exploded"),
        ):
            await _resolved(resolution)
        failure = next(e for e in logs if e["event"] == "workflow-deps-resolve-failed")
        assert (failure["workflow"], failure["generation"]) == ("wf-flaky", 1)
        assert failure["error_class"] == "RuntimeError"
        exc_info = failure.get("exc_info")
        assert isinstance(exc_info, BaseException)

        recovered = cast("_Probe", await _resolved(resolution))
        assert recovered.generation == 1
        await scope.teardown_worker_deps(worker_id)
        assert _RELEASES == [0, 1]

    asyncio.run(_scenario())


def test_a_generation_source_that_raises_keeps_serving_and_is_reported() -> None:
    """A broken generation read rotates nothing (the current instance keeps
    serving), is reported with its cause, and does not stop a co-resident
    binding from rotating."""
    broken = DepsFactory(_make_probe, generation=_failing_generation)
    healthy = DepsFactory(_make_probe, generation=_declared_generation)
    worker_id = new_uuid()

    async def _scenario() -> None:
        stuck = scope.prepare_worker_deps(broken, workflow="wf-broken", worker_id=worker_id)
        advancing = scope.prepare_worker_deps(healthy, workflow="wf-healthy", worker_id=worker_id)
        broken_instance = await _resolved(stuck)
        await _resolved(advancing)
        _GENERATION[0] = 1

        with structlog.testing.capture_logs() as logs:
            reports = await scope.rotate_worker_deps(worker_id, drain_timeout=1.0)

        assert [report.rotated for report in reports] == [False, True], reports
        failed = next(e for e in logs if e["event"] == "workflow-deps-rotate-failed")
        assert (failed["workflow"], failed["worker_id"]) == ("wf-broken", str(worker_id))
        assert "settings snapshot unavailable" in failed["error"]
        assert await _resolved(stuck) is broken_instance, "the current instance keeps serving"
        assert _RELEASES == [0], "only the healthy binding rotated"

        await scope.teardown_worker_deps(worker_id)
        assert _RELEASES == [0, 0], (
            "the broken binding's instance and the healthy one's superseded instance; "
            f"nothing was resolved on the healthy binding's new generation; got {_RELEASES}"
        )

    asyncio.run(_scenario())


# ── the direct door (real PG) ────────────────────────────────────────────


async def _new_flow(runner: FlowRunner) -> JobId:
    return (await runner.create_flow()).flow_id


async def test_the_direct_door_rotates_the_instance_on_an_explicit_reload(
    wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    """A long-lived ``FlowRunner`` over a generation-aware factory: an explicit
    reload mints the new generation for the next run, releases the superseded
    instance once, and leaves the runner's own ``aclose`` to release the live
    one."""
    app = _reload_app(["wf-direct"])
    runner = FlowRunner(app.get("wf-direct"), wf_pool, wf_schema)
    try:
        assert await runner.drive(await _new_flow(runner)) == "terminal"
        _GENERATION[0] = 1
        report = await runner.reload_deps(drain_timeout=5.0)
        assert report.rotated and report.drained
        assert (report.previous_generation, report.generation) == (0, 1)
        assert await runner.drive(await _new_flow(runner)) == "terminal"

        assert _PRODUCED[0] == 2
        assert [cast("_Probe", i).generation for i in _INSTANCES] == [0, 1]
        assert _INSTANCES[0] is not _INSTANCES[1]
        assert _RELEASES == [0], "only the superseded instance released by the reload"
    finally:
        await runner.aclose()
    assert _RELEASES == [0, 1], "the runner's aclose releases the live generation"


async def test_the_direct_door_refuses_to_rotate_an_instance_it_does_not_own(
    wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    """The explicit trigger names its owner: a runner with no factory has
    nothing to rotate, and the worker-injected resolution is the worker's to
    rotate (its own reload ordering owns the drain)."""
    static_app = WorkflowApp(deps=_Probe(asyncio.get_running_loop(), 0))

    @static_app.workflow("wf-direct-static")
    def _build_static() -> Promise[object]:
        return build(step(_deps_body, _Doc(doc_id="static"), key="solo"))

    static_runner = FlowRunner(static_app.get("wf-direct-static"), wf_pool, wf_schema)
    try:
        with pytest.raises(RuntimeError, match="deps_factory="):
            await static_runner.reload_deps(drain_timeout=1.0)
    finally:
        await static_runner.aclose()

    app = _reload_app(["wf-direct-injected"])
    compiled = app.get("wf-direct-injected")
    binding = compiled.deps_factory
    assert binding is not None
    worker_id = new_uuid()
    resolution = scope.prepare_worker_deps(
        binding, workflow="wf-direct-injected", worker_id=worker_id
    )
    injected = FlowRunner(
        compiled,
        wf_pool,
        wf_schema,
        worker_id=JobId(worker_id),
        deps_resolution=resolution,
    )
    try:
        with pytest.raises(RuntimeError, match="does not own"):
            await injected.reload_deps(drain_timeout=1.0)
    finally:
        await injected.aclose()
    await scope.teardown_worker_deps(worker_id)


async def test_the_direct_door_drains_a_running_body_before_releasing_its_instance(
    wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    """A reload landing on a body that is still executing: the body finishes
    cleanly against the old instance, a run started meanwhile uses the new
    generation, and the superseded instance is released only afterwards."""
    app = _reload_app(["wf-direct-drain"])
    runner = FlowRunner(app.get("wf-direct-drain"), wf_pool, wf_schema)
    _HOLD.append(asyncio.Event())
    try:
        slow = asyncio.create_task(runner.drive(await _new_flow(runner)))
        await asyncio.wait_for(_SIGNALS[0].started.wait(), timeout=30.0)
        assert cast("_Probe", _INSTANCES[0]).generation == 0

        _GENERATION[0] = 1
        reload_task = asyncio.create_task(runner.reload_deps(drain_timeout=30.0))
        await asyncio.wait_for(_SIGNALS[0].generation_read.wait(), timeout=30.0)
        assert not reload_task.done(), "the reload must wait for the running body"
        assert _RELEASES == []

        # a run claimed during the drain resolves the new generation.
        assert await runner.drive(await _new_flow(runner)) == "terminal"
        assert cast("_Probe", _INSTANCES[-1]).generation == 1
        assert _RELEASES == []

        _HOLD[0].set()
        assert await slow == "terminal"
        report = await asyncio.wait_for(reload_task, timeout=30.0)
        assert report.rotated and report.drained
        assert _RELEASES == [0], f"released after the last old body; got {_RELEASES}"
        assert _ORDER.index("body-end:wf-direct-drain") < _ORDER.index("deps-teardown:0")
    finally:
        _HOLD[0].set()
        await runner.aclose()
    assert _RELEASES == [0, 1]


async def test_the_direct_door_drains_a_detached_deadline_unwind(
    wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    """A start-to-close expiry detaches the body task, but its ``finally``
    can still be using deps after the attempt finalizes. The body lease must
    live until that task actually unwinds, not until the enforcement returns."""
    unwinding = asyncio.Event()
    finish_unwind = asyncio.Event()
    body_tasks: list[asyncio.Task[object]] = []

    async def _deadline_body(ctx: StepContext, doc: _Doc, deps: _Probe) -> _Report:
        del doc
        _INSTANCES.append(deps)
        try:
            await asyncio.sleep(30.0)
        finally:
            task = ctx._actor_body_task  # pyright: ignore[reportPrivateUsage]  # Why: the enforcement's public-to-the-runner task handle is how this pin reaps the detached body it deliberately creates.
            assert task is not None
            body_tasks.append(task)
            unwinding.set()
            await finish_unwind.wait()
            assert not deps.closed, "reload released deps under the detached body's finally"
        return _Report(ref="unreachable")

    app = WorkflowApp(deps_factory=_make_probe, deps_generation=_declared_generation)

    @app.workflow("wf-reload-deadline-unwind")
    def _flow() -> Promise[object]:
        return build(
            step(
                _deadline_body,
                _Doc(doc_id="deadline"),
                key="solo",
                max_attempts=1,
                start_to_close=timedelta(seconds=0.05),
            )
        )

    runner = FlowRunner(app.get("wf-reload-deadline-unwind"), wf_pool, wf_schema)
    reload_task: asyncio.Task[Any] | None = None
    try:
        assert await runner.drive(await _new_flow(runner)) == "terminal"
        await asyncio.wait_for(unwinding.wait(), timeout=5.0)
        assert len(body_tasks) == 1 and not body_tasks[0].done()

        _GENERATION[0] = 1
        reload_task = asyncio.create_task(runner.reload_deps(drain_timeout=30.0))
        await asyncio.wait_for(_SIGNALS[0].generation_read.wait(), timeout=5.0)
        assert not reload_task.done(), "reload stopped seeing the detached body's live lease"
        assert _RELEASES == []

        finish_unwind.set()
        await asyncio.gather(*body_tasks, return_exceptions=True)
        report = await asyncio.wait_for(reload_task, timeout=5.0)
        assert report.rotated and report.drained
        assert _RELEASES == [0]
    finally:
        finish_unwind.set()
        if body_tasks:
            await asyncio.gather(*body_tasks, return_exceptions=True)
        if reload_task is not None:
            await asyncio.wait_for(reload_task, timeout=5.0)
        await runner.aclose()


async def test_the_direct_door_reports_a_broken_generation_source_without_raising(
    wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    """The result object's error channel owns a broken generation read: the
    direct door keeps serving the current instance and a later reload can
    recover, exactly as the worker-hosted door does."""
    app = _reload_app(["wf-direct-broken-generation"], generation=_failing_generation)
    runner = FlowRunner(app.get("wf-direct-broken-generation"), wf_pool, wf_schema)
    try:
        assert await runner.drive(await _new_flow(runner)) == "terminal"
        first = _INSTANCES[0]

        report = await runner.reload_deps(drain_timeout=1.0)

        assert not report.rotated
        assert (report.previous_generation, report.generation) == (0, 0)
        assert isinstance(report.error, RuntimeError)
        assert "settings snapshot unavailable" in str(report.error)
        assert await runner.drive(await _new_flow(runner)) == "terminal"
        assert _PRODUCED[0] == 1
        assert [first, first] == _INSTANCES
        assert _RELEASES == [], "a failed generation read releases nothing"
    finally:
        await runner.aclose()
    assert _RELEASES == [0]


def _declare_reload_child_flows(
    app: WorkflowApp, *, child_app: WorkflowApp | None = None, suffix: str = ""
) -> str:
    """Parent and child workflows whose runners own their deps resolutions."""
    child_owner = app if child_app is None else child_app
    child_name = f"wf-reload-child{suffix}"
    parent_name = f"wf-reload-parent{suffix}"

    @child_owner.workflow(child_name)
    def _child() -> Promise[object]:
        return build(step(_deps_body, _Doc(doc_id="reload-child"), key="solo"))

    async def _parent_body(ctx: StepContext, doc: _Doc, deps: _Probe) -> _Report:
        del doc
        _INSTANCES.append(deps)
        handle = await ctx.spawn_flow(child_name, input=_Doc(doc_id="reload-child"), result=_Report)
        outcome = await handle.wait()
        assert isinstance(outcome, ChildOk), outcome
        return _Report(ref="parent")

    @app.workflow(parent_name)
    def _parent() -> Promise[object]:
        return build(step(_parent_body, _Doc(doc_id="reload-parent"), key="solo"))

    child_owner.get(child_name)
    return parent_name


@pytest.mark.parametrize("separate_child_app", [False, True], ids=["shared-app", "separate-app"])
async def test_the_direct_door_reload_releases_superseded_child_resolutions(
    wf_pool: asyncpg.Pool, wf_schema: str, separate_child_app: bool
) -> None:
    """A long-lived parent owns every child runner it minted. Reloading that
    parent must rotate and release those child resolutions too, not leave old
    credentials alive until some later process shutdown."""
    app = _reload_app([])
    child_app = _reload_app([]) if separate_child_app else None
    parent_name = _declare_reload_child_flows(
        app, child_app=child_app, suffix="-separate" if separate_child_app else ""
    )
    runner = FlowRunner(app.get(parent_name), wf_pool, wf_schema)
    try:
        assert await runner.drive(await _new_flow(runner), max_ticks=4000) == "terminal"
        assert _PRODUCED[0] == 2
        assert [cast("_Probe", instance).generation for instance in _INSTANCES] == [0, 0, 0]

        _GENERATION[0] = 1
        report = await runner.reload_deps(drain_timeout=5.0)

        assert report.rotated and report.drained
        assert sorted(_RELEASES) == [0, 0], (
            "the parent and its completed child both superseded one resolved instance; "
            f"got releases {_RELEASES}"
        )
        assert await runner.drive(await _new_flow(runner), max_ticks=4000) == "terminal"
        assert _PRODUCED[0] == 4
        assert [cast("_Probe", instance).generation for instance in _INSTANCES[3:]] == [1, 1, 1]
    finally:
        await runner.aclose()
    assert sorted(_RELEASES) == [0, 0, 1, 1]


async def test_a_static_parent_reload_rotates_its_factory_bound_child(
    wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    """The parent need not declare deps itself to own a factory-bound child
    runner. Its explicit reload is the only public trigger reaching that
    child, so refusal would strand the child's superseded resource."""
    parent_app = WorkflowApp()
    child_app = _reload_app([])
    child_name = "wf-reload-child-static-parent"
    parent_name = "wf-reload-parent-static-parent"

    @child_app.workflow(child_name)
    def _child() -> Promise[object]:
        return build(step(_deps_body, _Doc(doc_id="reload-child"), key="solo"))

    async def _parent_body(ctx: StepContext, doc: _Doc) -> _Report:
        handle = await ctx.spawn_flow(child_name, input=doc, result=_Report)
        outcome = await handle.wait()
        assert isinstance(outcome, ChildOk), outcome
        return _Report(ref="parent")

    @parent_app.workflow(parent_name)
    def _parent() -> Promise[object]:
        return build(step(_parent_body, _Doc(doc_id="reload-parent"), key="solo"))

    child_app.get(child_name)
    runner = FlowRunner(parent_app.get(parent_name), wf_pool, wf_schema)
    try:
        assert await runner.drive(await _new_flow(runner), max_ticks=4000) == "terminal"
        assert _PRODUCED[0] == 1
        assert [cast("_Probe", instance).generation for instance in _INSTANCES] == [0]

        _GENERATION[0] = 1
        report = await runner.reload_deps(drain_timeout=5.0)

        assert report.rotated and report.drained
        assert (report.previous_generation, report.generation) == (0, 1)
        assert _RELEASES == [0]
        assert await runner.drive(await _new_flow(runner), max_ticks=4000) == "terminal"
        assert _PRODUCED[0] == 2
        assert [cast("_Probe", instance).generation for instance in _INSTANCES[1:]] == [1]
    finally:
        await runner.aclose()
    assert sorted(_RELEASES) == [0, 1]


async def test_the_direct_door_tolerates_a_child_created_during_reload() -> None:
    """A concurrent drive may add a child runner while reload is draining an
    earlier child. The ownership walk must iterate a stable snapshot rather
    than fail with ``dictionary changed size during iteration``."""
    reads = 0
    child_rotation_started = asyncio.Event()

    def _generation() -> int:
        nonlocal reads
        reads += 1
        if reads == 2:
            child_rotation_started.set()
        return _GENERATION[0]

    app = _reload_app(["wf-reload-child-iteration"], generation=_generation)
    compiled = app.get("wf-reload-child-iteration")
    root = FlowRunner(compiled, cast("Any", object()), "unused_schema")
    first_child = FlowRunner(compiled, cast("Any", object()), "unused_schema")
    root._child_runners["first"] = first_child  # pyright: ignore[reportPrivateUsage]  # Why: the pin controls the exact child-creation interleaving the public reload walk must tolerate.
    lease = first_child._deps_scope(_deps_body, 1)  # pyright: ignore[reportPrivateUsage]
    await lease.__aenter__()
    _GENERATION[0] = 1
    reload_task = asyncio.create_task(root.reload_deps(drain_timeout=30.0))
    lease_open = True
    try:
        await asyncio.wait_for(child_rotation_started.wait(), timeout=5.0)
        second_child = FlowRunner(compiled, cast("Any", object()), "unused_schema")
        root._child_runners["second"] = second_child  # pyright: ignore[reportPrivateUsage]
        await lease.__aexit__(None, None, None)
        lease_open = False
        report = await asyncio.wait_for(reload_task, timeout=5.0)
        assert report.rotated
    finally:
        if lease_open:
            await lease.__aexit__(None, None, None)
        await asyncio.gather(reload_task, return_exceptions=True)
        await root.aclose()


# ── the worker door over the real execution door (real PG) ───────────────


def _claimed_job(
    job_id: JobId,
    *,
    metadata: dict[str, object],
    payload: object = None,
    attempt: int = 1,
    claim_epoch: int = 1,
) -> JobRow:
    return JobRow(
        id=job_id,
        actor="wf-reload",
        queue="default",
        identity_key=None,
        fairness_key=None,
        payload=payload if payload is not None else {},
        payload_schema_ver=0,
        status="running",
        priority=0,
        attempt=attempt,
        max_attempts=3,
        retry_kind="transient",
        created_at=datetime(2025, 1, 1, tzinfo=UTC),
        scheduled_at=datetime(2025, 1, 1, tzinfo=UTC),
        metadata=metadata,
        claim_epoch=claim_epoch,
    )


async def _create_run(pool: asyncpg.Pool, schema: str, compiled: Any) -> JobId:
    async with FlowRunner(compiled, pool, schema) as runner:
        return (await runner.create_flow()).flow_id


async def _claim_and_execute(
    pool: asyncpg.Pool, schema: str, flow_id: JobId, worker_id: UUID
) -> seam.FlowExecution:
    """One claimed node row through the real execution door: the row goes
    running under *worker_id* (the dispatch claim's own shape), then
    ``execute_flow_job`` resolves and runs the body."""
    row = await pool.fetchrow(
        f'SELECT id, payload FROM "{schema}".jobs '  # noqa: S608  # Why: the schema identifier is the fixture's own (validated against _IDENT_RE); asyncpg cannot bind identifiers.
        "WHERE step_key = 'solo' AND (metadata->>'flow_id')::uuid = $1",
        flow_id,
    )
    assert row is not None, f"no node row for flow {flow_id}"
    await pool.execute(
        f"UPDATE \"{schema}\".jobs SET status = 'running', attempt = 1, claim_epoch = 1, "  # noqa: S608  # Why: same fixture-owned schema identifier.
        "locked_by_worker = $2, lock_expires_at = now() + interval '60 seconds' WHERE id = $1",
        row["id"],
        worker_id,
    )
    payload = json.loads(row["payload"]) if isinstance(row["payload"], str) else row["payload"]
    return await seam.execute_flow_job(
        pool=pool,
        schema=schema,
        worker_id=JobId(worker_id),
        job=_claimed_job(JobId(row["id"]), metadata={"flow_id": str(flow_id)}, payload=payload),
    )


@pytest.mark.integration
async def test_the_worker_reload_hand_off_rotates_the_deps_and_rebuilds_the_runner(
    wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    """Through the real fleet-claimed door: a reload rotates the worker-scoped
    instance, the next claim resolves the new generation, the cached runner was
    rebuilt, and the shutdown teardown releases only what is still live."""
    app = _reload_app(["wf-worker-reload"])
    compiled = app.get("wf-worker-reload")
    binding = compiled.deps_factory
    assert binding is not None
    worker_id = new_uuid()
    pool = wf_pool

    def _runner_identity() -> object:
        resolution = scope.prepare_worker_deps(
            binding, workflow="wf-worker-reload", worker_id=worker_id
        )
        return seam._runner_for(  # pyright: ignore[reportPrivateUsage]  # Why: the door's runner cache is the surface whose rebuild the reload promises.
            "wf-worker-reload",
            pool,
            wf_schema,
            JobId(worker_id),
            deps_resolution=resolution,
        )

    first_flow = await _create_run(wf_pool, wf_schema, compiled)
    before_runner = _runner_identity()
    execution = await _claim_and_execute(wf_pool, wf_schema, first_flow, worker_id)
    assert execution.outcome == "succeeded", execution
    assert _PRODUCED[0] == 1

    _GENERATION[0] = 1
    await seam.reload_worker_workflow_state(worker_id, drain_timeout=5.0)

    after_runner = _runner_identity()
    assert after_runner is not before_runner, "the reload evicted the stale cached runner"
    assert _RELEASES == [0]

    second_flow = await _create_run(wf_pool, wf_schema, compiled)
    execution = await _claim_and_execute(wf_pool, wf_schema, second_flow, worker_id)
    assert execution.outcome == "succeeded", execution
    assert _PRODUCED[0] == 2
    assert [cast("_Probe", i).generation for i in _INSTANCES] == [0, 1]

    await seam.teardown_worker_workflow_state(worker_id)
    await seam.teardown_worker_workflow_state(worker_id)
    assert _RELEASES == [0, 1], f"one release per resolved instance; got {_RELEASES}"


@pytest.mark.integration
async def test_a_worker_reload_leaves_a_co_resident_workers_deps_alone(
    wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    """Two workers of one process share the loop but never the scope: a reload
    rotates only the reloading worker's instance."""
    app = _reload_app(["wf-worker-neighbour"])
    compiled = app.get("wf-worker-neighbour")
    binding = compiled.deps_factory
    assert binding is not None
    reloading, neighbour = new_uuid(), new_uuid()

    for worker_id in (reloading, neighbour):
        flow_id = await _create_run(wf_pool, wf_schema, compiled)
        execution = await _claim_and_execute(wf_pool, wf_schema, flow_id, worker_id)
        assert execution.outcome == "succeeded", execution
    assert _PRODUCED[0] == 2
    mine, theirs = cast("_Probe", _INSTANCES[0]), cast("_Probe", _INSTANCES[1])
    assert mine is not theirs

    _GENERATION[0] = 1
    await seam.reload_worker_workflow_state(reloading, drain_timeout=5.0)

    assert _RELEASES == [0], "only the reloading worker's instance released"
    still_theirs = scope.prepare_worker_deps(
        binding, workflow="wf-worker-neighbour", worker_id=neighbour
    )
    assert await _resolved(still_theirs) is theirs, "the neighbour keeps serving its own instance"

    await seam.teardown_worker_workflow_state(reloading)
    await seam.teardown_worker_workflow_state(neighbour)
    assert _RELEASES == [0, 0], (
        "one release each: the reloading worker rotated its instance (nothing resolved on its "
        f"new generation before teardown) and the neighbour released its own; got {_RELEASES}"
    )


# ── the reload/claim gate (the pool-lifetime owner) ──────────────────────


async def test_the_reload_gate_waits_only_for_claims_on_the_pool_it_asks_about() -> None:
    """A claim holds the pool it was admitted with: the reload waits for that
    pool's claims and no others — a claim's lifetime on one pool must never
    hold up the close of a different, idle one."""
    gate = worker_deps_mod.ReloadGate()
    busy_pool, idle_pool = _FakePool("busy"), _FakePool("idle")
    release = asyncio.Event()
    entered = asyncio.Event()

    async def _claim() -> None:
        async with gate.claim(busy_pool) as held:
            assert held is busy_pool, "the gate answers the pool it holds"
            entered.set()
            await release.wait()

    claim = asyncio.create_task(_claim())
    await asyncio.wait_for(entered.wait(), timeout=5.0)

    assert await gate.await_quiet(idle_pool, drain_timeout=1.0) is True, (
        "a pool with no claims is quiet immediately"
    )
    waiting = asyncio.create_task(gate.await_quiet(busy_pool, drain_timeout=30.0))
    await asyncio.sleep(0)
    assert not waiting.done(), "the busy pool's claim still defers its close"

    release.set()
    await claim
    assert await asyncio.wait_for(waiting, timeout=5.0) is True


async def test_the_reload_gate_bound_expires_into_the_force_proceed() -> None:
    """A claim outliving the bound answers ``False`` (the caller force-closes,
    the documented escape hatch) and a later claim re-opens the pool rather
    than inheriting the stale quiet."""
    gate = worker_deps_mod.ReloadGate()
    pool = _FakePool("stuck")

    async with gate.claim(pool):
        assert await gate.await_quiet(pool, drain_timeout=0.02) is False

    assert await gate.await_quiet(pool, drain_timeout=1.0) is True, (
        "the released claim leaves the pool quiet"
    )
    async with gate.claim(pool):
        assert await gate.await_quiet(pool, drain_timeout=0.02) is False, (
            "a fresh claim on the pool defers the next close again"
        )


async def test_the_old_pool_close_reports_a_claim_timeout_and_force_proceeds() -> None:
    """The close call site, not only the gate primitive, owns the escape
    hatch: a live claim past the bound produces structured timeout telemetry
    and the old pool still closes rather than remaining alive indefinitely."""
    gate = worker_deps_mod.ReloadGate()
    pool = _FakePool("dispatcher")
    claim = gate.claim(pool)
    await claim.__aenter__()
    try:
        with structlog.testing.capture_logs() as logs:
            worker_deps_mod._drain_old_pool(  # pyright: ignore[reportPrivateUsage]  # Why: the timeout log and forced close are this background close seam's observable contract, not the gate primitive's alone.
                pool, "dispatcher", 0.02, gate=gate
            )
            await wait_for(
                pool.closed_event,
                timeout=5.0,
                description="the old pool's forced close after the claim drain bound",
            )

        timeout = next(entry for entry in logs if entry["event"] == "pool-drain-claims-timeout")
        assert (timeout["pool"], timeout["drain_timeout"]) == ("dispatcher", 0.02)
        assert pool.closed
    finally:
        await claim.__aexit__(None, None, None)


# ── the reload ordering contract inside reload_credentials (fakes) ───────


class _FakePool:
    """Fake asyncpg.Pool: ``close()`` records its label on the shared order
    list (the ``_close_events`` convention ``test_worker_deps_teardown``
    established) and signals closure through an event, so ordering pins never
    sleep."""

    def __init__(self, name: str, order: list[str] | None = None) -> None:
        self.name = name
        self.closed = False
        self.terminated = False
        self.close_wait = asyncio.Event()
        self.close_wait.set()
        self.closed_event = asyncio.Event()
        self._order = order

    async def close(self) -> None:
        if self.closed or self.terminated:
            return
        await self.close_wait.wait()
        self.closed = True
        self.closed_event.set()
        if self._order is not None:
            self._order.append(f"pool:{self.name}")

    def terminate(self) -> None:
        self.terminated = True
        self.closed = True
        self.closed_event.set()
        self.close_wait.set()

    def is_closing(self) -> bool:
        return self.closed

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()


class _FakeConn:
    async def execute(self, sql: str, *_args: object) -> str:
        return "OK"

    async def add_listener(self, channel: str, callback: object) -> None:
        del channel, callback

    async def remove_listener(self, channel: str, callback: object) -> None:
        del channel, callback

    async def close(self) -> None:
        return

    def is_closed(self) -> bool:
        return True


def _fake_settings() -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": "postgresql://fake:fake@fake:5432/fake",
            "TASKQ_PG_DSN_DIRECT": "postgresql://fake:fake@fake:5432/fake",
            "TASKQ_PG_DSN_POOLED": "postgresql://fake:fake@fake:5432/fake",
            "TASKQ_HEALTH_ENABLED": "false",
            "TASKQ_NOTIFY_ENABLED": "false",
        }
    )


def _pool_factory(pools: list[_FakePool]) -> Callable[[], Awaitable[_FakePool]]:
    async def _next() -> _FakePool:
        return pools.pop(0)

    return _next


def _fake_connections(*, order: list[str]) -> tuple[WorkerConnections, dict[str, _FakePool]]:
    old = {label: _FakePool(label, order) for label in ("dispatcher", "heartbeat", "worker")}
    new = {label: _FakePool(f"{label}-2", order) for label in old}
    conns = WorkerConnections(
        dispatcher_pool_factory=_pool_factory([old["dispatcher"], new["dispatcher"]]),  # type: ignore[arg-type]  # Why: the fake satisfies the pool surface the reload uses, not asyncpg's C-extension type.
        heartbeat_pool_factory=_pool_factory([old["heartbeat"], new["heartbeat"]]),  # type: ignore[arg-type]
        worker_pool_factory=_pool_factory([old["worker"], new["worker"]]),  # type: ignore[arg-type]
        notify_conn=_FakeConn(),  # type: ignore[arg-type]
        leader_conn=_FakeConn(),  # type: ignore[arg-type]
    )
    return conns, old


async def test_the_reload_drain_runs_before_any_old_pool_closes() -> None:
    """``reload_credentials`` awaits the pre-swap drain FIRST: the workflow
    deps' superseded instance is released while every old pool is still open,
    and only then do the old pools start closing."""
    order: list[str] = []
    conns, old = _fake_connections(order=order)
    async with worker_deps_mod.open_worker_deps(_fake_settings(), connections=conns) as deps:
        boot_dispatcher = deps.dispatcher_pool

        async def _drain(drain_timeout: float) -> None:
            assert drain_timeout == 0.25
            assert deps.dispatcher_pool is boot_dispatcher, "the drain runs before the swap"
            assert not old["dispatcher"].closed, "the superseded deps release precedes the close"
            order.append("deps-teardown")

        deps.reload_drain = _drain
        await worker_deps_mod.reload_credentials(deps, drain_timeout=0.25)
        assert deps.dispatcher_pool is not boot_dispatcher, "the pools were swapped"
        for pool in old.values():
            await wait_for(
                pool.closed_event,
                timeout=5.0,
                description=f"the reload's background drain closing {pool.name!r}",
            )

    assert order[0] == "deps-teardown", f"the pre-swap drain ran first; got {order}"
    assert set(order[1:4]) == {"pool:dispatcher", "pool:heartbeat", "pool:worker"}, order


async def test_a_pool_reload_without_a_generation_change_also_waits_for_claims() -> None:
    """Pool lifetime is independent of whether the deps instance rotates. An
    unchanged generation must not turn a credential reload into an immediate
    old-pool close while a claim still uses that pool."""
    binding = DepsFactory(_make_probe, generation=_declared_generation)
    worker_id = new_uuid()
    resolution = scope.prepare_worker_deps(
        binding, workflow="wf-pool-only-reload", worker_id=worker_id
    )
    await _resolved(resolution)
    claim_hold = resolution.hold()
    await claim_hold.__aenter__()
    hold_open = True
    conns, old = _fake_connections(order=[])
    reload_task: asyncio.Task[tuple[list[str], list[str]]] | None = None
    try:
        async with worker_deps_mod.open_worker_deps(_fake_settings(), connections=conns) as deps:

            async def _drain(drain_timeout: float) -> None:
                await seam.reload_worker_workflow_state(worker_id, drain_timeout=drain_timeout)

            deps.reload_drain = _drain
            reload_task = asyncio.create_task(
                worker_deps_mod.reload_credentials(deps, drain_timeout=0.05)
            )
            await asyncio.wait_for(_SIGNALS[0].generation_read.wait(), timeout=5.0)
            await asyncio.sleep(0)  # let the reload schedule its old-pool closes
            await asyncio.sleep(0)  # let an unguarded close reach the fake pool
            assert not old["worker"].closed and not old["worker"].terminated, (
                "the old pool closed while its claim was still live"
            )
            await claim_hold.__aexit__(None, None, None)
            hold_open = False
            await asyncio.wait_for(reload_task, timeout=5.0)
    finally:
        if hold_open:
            await claim_hold.__aexit__(None, None, None)
        if reload_task is not None:
            await asyncio.gather(reload_task, return_exceptions=True)
        await scope.teardown_worker_deps(worker_id)


async def test_a_raising_reload_drain_is_reported_and_the_rotation_continues() -> None:
    """A failing drain is logged with its cause and does not abort the reload:
    the pools are still rotated, and a later SIGHUP retries the drain."""
    order: list[str] = []
    conns, _old = _fake_connections(order=order)

    async def _broken_drain(drain_timeout: float) -> None:
        raise RuntimeError("drain backend unavailable")

    async with worker_deps_mod.open_worker_deps(_fake_settings(), connections=conns) as deps:
        boot_dispatcher = deps.dispatcher_pool
        deps.reload_drain = _broken_drain
        with structlog.testing.capture_logs() as logs:
            reloaded, failed = await worker_deps_mod.reload_credentials(deps, drain_timeout=0.1)

        assert failed == [], failed
        assert sorted(reloaded) == ["dispatcher", "heartbeat", "worker"], reloaded
        assert deps.dispatcher_pool is not boot_dispatcher
        failure = next(e for e in logs if e["event"] == "credential-reload-drain-failed")
        assert "drain backend unavailable" in failure["error"]


async def test_the_reload_drain_runs_without_any_credential_factory() -> None:
    """A settings-only reload — every role resource caller-owned, so
    ``reload_credentials`` has no factory to rotate (the deployment with no PG
    credential provider) — still runs the workflow deps drain with the
    reload's bound."""
    conns = WorkerConnections(
        dispatcher_pool=_FakePool("dispatcher"),  # type: ignore[arg-type]  # Why: the fake satisfies the pool surface the reload reads, not asyncpg's C-extension type.
        heartbeat_pool=_FakePool("heartbeat"),  # type: ignore[arg-type]
        worker_pool=_FakePool("worker"),  # type: ignore[arg-type]
        notify_conn=_FakeConn(),  # type: ignore[arg-type]
        leader_conn=_FakeConn(),  # type: ignore[arg-type]
    )
    async with worker_deps_mod.open_worker_deps(_fake_settings(), connections=conns) as deps:
        calls: list[float] = []

        async def _drain(drain_timeout: float) -> None:
            calls.append(drain_timeout)

        deps.reload_drain = _drain
        reloaded, failed = await worker_deps_mod.reload_credentials(deps, drain_timeout=2.5)

    assert calls == [2.5], "the deps rotation runs even when nothing else rotates"
    assert (reloaded, failed) == ([], []), "caller-owned resources are skipped, not failed"


# ── the real worker process (``_bootstrap._main``) ───────────────────────


def _real_pool_connections(pg_dsn: str) -> tuple[WorkerConnections, list[asyncpg.Pool]]:
    """The factory-backed role pools a credential-provider deployment boots
    with: each factory call builds a fresh real pool, so a reload genuinely
    swaps the worker's pools (a DSN-baked deployment has nothing to rotate and
    would vacuously pass the ordering pins)."""
    created: list[asyncpg.Pool] = []

    def _pool_factory() -> Callable[[], Awaitable[asyncpg.Pool]]:
        async def _make() -> asyncpg.Pool:
            pool = await asyncpg.create_pool(pg_dsn, min_size=1)
            assert pool is not None
            created.append(pool)
            return pool

        return _make

    return (
        WorkerConnections(
            dispatcher_pool_factory=_pool_factory(),
            heartbeat_pool_factory=_pool_factory(),
            worker_pool_factory=_pool_factory(),
        ),
        created,
    )


def _worker_settings(
    pg_dsn: str, schema: str, *, tag: str, max_concurrency: int = 1
) -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": pg_dsn,
            "TASKQ_SCHEMA_NAME": schema,
            "TASKQ_MAX_CONCURRENCY": str(max_concurrency),
            "TASKQ_HEALTH_ENABLED": "false",
            "TASKQ_HEALTH_SOCKET_PATH": unique_health_sock_path(f"reload_{tag}"),
            "TASKQ_LOOP_LAG_WARN_BUDGET": "9999",
            "TASKQ_LOOP_LAG_BUDGET": "9999",
        }
    )


def _capture_worker_deps(monkeypatch: pytest.MonkeyPatch) -> list[Any]:
    """Record each deps shell ``_main`` opens — the test's handle on the running
    worker's ``request_reload()`` (the programmatic SIGHUP)."""
    captured: list[Any] = []
    real_open = open_worker_deps

    @contextlib.asynccontextmanager
    async def _open(
        settings: WorkerSettings, *, connections: Any = None
    ) -> AsyncGenerator[Any, None]:
        async with real_open(settings, connections=connections) as deps:
            captured.append(deps)
            yield deps

    monkeypatch.setattr(bootstrap_mod, "open_worker_deps", _open)
    return captured


def _tap_pool_closes(monkeypatch: pytest.MonkeyPatch) -> None:
    """Record each role pool's bounded close on the shared marker list — the
    before-pool-close proof (``test_worker_deps_teardown``'s seam)."""
    real_close = worker_deps_mod.close_pool_bounded  # pyright: ignore[reportPrivateImportUsage]  # Why: the close site reads this module global at call time; capturing it is how the tap keeps calling the real bounded close.

    async def _tapped_close(
        pool: asyncpg.Pool, label: str, close_timeout: float, *, family: Any = "teardown"
    ) -> None:
        _ORDER.append(f"pool:{label}")
        await real_close(pool, label, close_timeout, family=family)

    monkeypatch.setattr(worker_deps_mod, "close_pool_bounded", _tapped_close)  # pyright: ignore[reportPrivateImportUsage]  # Why: the call site reads this module global at close time, so replacing it here is the only way to observe the pool-close order.
    monkeypatch.setattr(bootstrap_mod, "close_pool_bounded", _tapped_close)


def _assert_no_loop_or_pool_errors(logs: list[dict[str, Any]]) -> None:
    forbidden = ("Event loop is closed", "attached to a different loop", "pool is closed")
    offending = [entry for entry in logs if any(text in repr(entry) for text in forbidden)]
    assert offending == [], offending


@pytest.mark.integration
async def test_a_rolling_reload_rotates_the_deps_before_the_old_pool_closes(
    module_pg_schema: ModulePgSchema,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The real worker process during a rolling reload.

    A slow ``G1`` body is executing when SIGHUP (``deps.request_reload()``)
    arrives; it finishes inside the drain bound, against the old instance and
    while the old pools are still open. The next claim resolves the new
    generation on the swapped pool, and the superseded deps instance was
    released exactly once — strictly before any role pool closed.
    """
    schema = module_pg_schema.schema_name
    app = _reload_app(["wf-roll"])
    compiled = app.get("wf-roll")
    pool = await asyncpg.create_pool(module_pg_schema.pg_dsn)
    _HOLD.append(asyncio.Event())
    try:
        first = await _create_run(pool, schema, compiled)
        second = await _create_run(pool, schema, compiled)
        _tap_pool_closes(monkeypatch)
        captured = _capture_worker_deps(monkeypatch)
        settings = _worker_settings(module_pg_schema.pg_dsn, schema, tag="roll")
        connections, _pools = _real_pool_connections(module_pg_schema.pg_dsn)

        with structlog.testing.capture_logs() as logs:
            worker = asyncio.create_task(
                bootstrap_mod._main(  # pyright: ignore[reportPrivateUsage]  # Why: the real worker entry point is the composition under test.
                    settings,
                    actor_registry={},
                    connections=connections,
                    until_idle=True,
                    idle_settle_window=0.2,
                    idle_poll_interval=0.1,
                    idle_max_runtime=120.0,
                )
            )
            await wait_for_condition(
                lambda: len(_EXECUTED) == 1,
                description="the worker claimed and started the slow workflow body",
                timeout=60.0,
            )
            boot_pool = captured[0].dispatcher_pool
            _GENERATION[0] = 1  # the settings/credential version advanced
            captured[0].request_reload()
            await wait_for_condition(
                lambda: "generation-read" in _ORDER,
                description="the reload reached the deps rotation",
                timeout=30.0,
            )
            assert "body-end:wf-roll" not in _ORDER, (
                "the rotation must start while the slow body is still running"
            )
            _HOLD[0].set()
            await wait_for_condition(
                lambda: "deps-teardown:0" in _ORDER,
                description="the superseded generation was released",
                timeout=30.0,
            )
            await wait_for_condition(
                lambda: captured[0].dispatcher_pool is not boot_pool,
                description="the reload swapped the worker's pool",
                timeout=30.0,
            )
            await wait_for_condition(
                lambda: len(_EXECUTED) == 2,
                description="the post-reload claim executed on the new generation",
                timeout=60.0,
            )
            await wait_for_condition(
                lambda: captured[0].active_jobs.count() == 0,
                description="the post-reload claim's finalize completed",
                timeout=60.0,
            )
            for flow_id in (first, second):
                await pool.execute(
                    f"UPDATE \"{schema}\".jobs SET status = 'succeeded', "  # noqa: S608  # Why: the schema identifier is the fixture's own.
                    "finished_at = clock_timestamp() WHERE id = $1 AND step_key = '__flow__'",
                    flow_id,
                )
            exit_code = await asyncio.wait_for(worker, timeout=120.0)

        assert exit_code == 0, exit_code
        assert _PRODUCED[0] == 2, f"one mint per generation; got {_PRODUCED[0]}"
        assert [cast("_Probe", i).generation for i in _INSTANCES] == [0, 1]
        assert _RELEASES == [0, 1], f"rotation then shutdown, one release each; got {_RELEASES}"
        assert _ORDER.index("deps-teardown:0") < _ORDER.index("pool:dispatcher"), (
            f"the superseded deps release precedes the old pool's close; order={_ORDER}"
        )
        _assert_no_loop_or_pool_errors(logs)

        resolved = [e for e in logs if e["event"] == "workflow-deps-resolved"]
        assert [e["generation"] for e in resolved] == [0, 1], resolved
        rotated = next(e for e in logs if e["event"] == "workflow-deps-rotated")
        assert (rotated["previous_generation"], rotated["generation"]) == (0, 1)
        assert not [e for e in logs if e["event"] == "workflow-deps-drain-timeout"]
    finally:
        _HOLD[0].set()
        await pool.close()


@pytest.mark.integration
async def test_a_stuck_body_past_the_drain_bound_is_force_released_once(
    module_pg_schema: ModulePgSchema,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A body that outlives the drain bound during a rolling reload.

    The timeout is reported with ``workflow``, ``worker_id`` and
    ``generation``; the bounded release runs exactly once, strictly before the
    old pool is terminated; and when the stuck body later unwinds, no second
    teardown and no leaked instance remain.
    """
    schema = module_pg_schema.schema_name
    app = _reload_app(["wf-stuck"])
    compiled = app.get("wf-stuck")
    pool = await asyncpg.create_pool(module_pg_schema.pg_dsn)
    _HOLD.append(asyncio.Event())
    real_reload = worker_deps_mod.reload_credentials

    async def _tight_reload(deps: Any, **kwargs: Any) -> Any:
        # The production reload with a TIGHT drain bound: the review gate runs
        # the shipped 5 s default, a test cannot wait it out per assertion and
        # the bound is the function's own parameter.
        return await real_reload(deps, drain_timeout=0.2, **kwargs)

    monkeypatch.setattr(worker_deps_mod, "reload_credentials", _tight_reload)
    try:
        flow_id = await _create_run(pool, schema, compiled)
        _tap_pool_closes(monkeypatch)
        captured = _capture_worker_deps(monkeypatch)
        settings = _worker_settings(module_pg_schema.pg_dsn, schema, tag="stuck")
        connections, _pools = _real_pool_connections(module_pg_schema.pg_dsn)

        with structlog.testing.capture_logs() as logs:
            worker = asyncio.create_task(
                bootstrap_mod._main(  # pyright: ignore[reportPrivateUsage]  # Why: the real worker entry point is the composition under test.
                    settings,
                    actor_registry={},
                    connections=connections,
                    until_idle=True,
                    idle_settle_window=0.2,
                    idle_poll_interval=0.1,
                    idle_max_runtime=120.0,
                )
            )
            await wait_for_condition(
                lambda: len(_EXECUTED) == 1,
                description="the worker claimed and started the stuck workflow body",
                timeout=60.0,
            )
            _GENERATION[0] = 1  # the settings/credential version advanced
            captured[0].request_reload()
            await wait_for_condition(
                lambda: "deps-teardown:0" in _ORDER,
                description="the drain bound expired and the superseded deps were released",
                timeout=30.0,
            )
            # the forced release happened while the body was still stuck
            assert "body-end:wf-stuck" not in _ORDER, _ORDER

            _HOLD[0].set()
            await wait_for_condition(
                lambda: captured[0].active_jobs.count() == 0,
                description="the stuck claim unwound",
                timeout=60.0,
            )
            # the stuck claim's rows are settled so the drain monitor can see
            # an idle queue (a failed finalize would otherwise hold them until
            # the lost-claim reconcile, which this pin does not exercise).
            for step_key in ("solo", "__flow__"):
                await pool.execute(
                    f"UPDATE \"{schema}\".jobs SET status = 'succeeded', "  # noqa: S608  # Why: the schema identifier is the fixture's own.
                    "finished_at = clock_timestamp() WHERE id = $1 AND step_key = $2",
                    flow_id,
                    step_key,
                )
            exit_code = await asyncio.wait_for(worker, timeout=120.0)

        # The stuck claim's own finalize raced the old pool's termination: a
        # clean completion or a counted job failure are both legitimate
        # verdicts, and neither is a hang or a watchdog kill.
        assert exit_code in (0, 3), exit_code
        timeout = [e for e in logs if e["event"] == "workflow-deps-drain-timeout"]
        assert len(timeout) == 1, timeout
        worker_logs = [e for e in timeout if e["event"] == "workflow-deps-drain-timeout"]
        assert worker_logs[0]["workflow"] == "wf-stuck"
        assert worker_logs[0]["generation"] == 0
        assert worker_logs[0]["worker_id"], "the timeout names the worker that drained"
        assert worker_logs[0]["drain_timeout"] == 0.2
        assert _RELEASES == [0], (
            f"exactly one bounded release, and the stuck unwind released nothing more; got {_RELEASES}"
        )
        assert [e["event"] for e in logs if e["event"] == "workflow-deps-torn-down"] == [
            "workflow-deps-torn-down"
        ]
        assert _ORDER.index("deps-teardown:0") < _ORDER.index("pool:dispatcher"), (
            f"the forced deps release precedes the old pool's close; order={_ORDER}"
        )
    finally:
        _HOLD[0].set()
        await pool.close()


@pytest.mark.integration
async def test_a_claim_admitted_during_rotation_keeps_its_pool_until_it_finishes(
    module_pg_schema: ModulePgSchema,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A claim admitted during the drain uses the new deps generation but the
    not-yet-swapped worker pool. Its pool lifetime must therefore be protected
    too; draining only the superseded generation closes that pool under it.
    """
    schema = module_pg_schema.schema_name
    old_started, new_started = asyncio.Event(), asyncio.Event()
    release_old, release_new = asyncio.Event(), asyncio.Event()
    body_errors: list[Exception] = []

    async def _overlapping_body(ctx: StepContext, doc: _Doc, deps: _Probe) -> _Report:
        del doc
        _INSTANCES.append(deps)
        if deps.generation == 0:
            old_started.set()
            await release_old.wait()
        else:
            new_started.set()
            await release_new.wait()
        try:
            async with ctx._pool.acquire() as conn:  # pyright: ignore[reportPrivateUsage]  # Why: the body's own pool is the resource whose lifetime this integration pin proves.
                assert await conn.fetchval("SELECT 1") == 1
        except Exception as exc:
            body_errors.append(exc)
            raise
        return _Report(ref=f"generation-{deps.generation}")

    app = WorkflowApp(deps_factory=_make_probe, deps_generation=_declared_generation)

    @app.workflow("wf-reload-overlapping-claims")
    def _flow() -> Promise[object]:
        return build(
            step(
                _overlapping_body,
                _Doc(doc_id="overlap"),
                key="solo",
                actor="wf-reload",
                queue="default",
                max_attempts=1,
            )
        )

    compiled = app.get("wf-reload-overlapping-claims")
    pool = await asyncpg.create_pool(module_pg_schema.pg_dsn)
    worker: asyncio.Task[int] | None = None
    old_pool_closed = asyncio.Event()
    old_dispatcher_closed = asyncio.Event()
    dispatcher_wait_started = asyncio.Event()
    boot_worker_pool: asyncpg.Pool | None = None
    boot_dispatcher_pool: asyncpg.Pool | None = None
    real_reload = worker_deps_mod.reload_credentials

    async def _tight_reload(deps: Any, **kwargs: Any) -> Any:
        return await real_reload(deps, drain_timeout=0.2, **kwargs)

    monkeypatch.setattr(worker_deps_mod, "reload_credentials", _tight_reload)
    real_close = worker_deps_mod.close_pool_bounded  # pyright: ignore[reportPrivateImportUsage]  # Why: the production close site reads this module global at call time.

    async def _close_and_signal(
        closing: asyncpg.Pool, label: str, close_timeout: float, *, family: Any = "teardown"
    ) -> None:
        await real_close(closing, label, close_timeout, family=family)
        if closing is boot_worker_pool and label == "worker":
            old_pool_closed.set()
        if closing is boot_dispatcher_pool and label == "dispatcher":
            old_dispatcher_closed.set()

    real_await_quiet = worker_deps_mod.ReloadGate.await_quiet

    async def _wait_and_signal(
        gate: worker_deps_mod.ReloadGate, pool: object, *, drain_timeout: float
    ) -> bool:
        if pool is boot_dispatcher_pool:
            dispatcher_wait_started.set()
        return await real_await_quiet(gate, pool, drain_timeout=drain_timeout)

    monkeypatch.setattr(worker_deps_mod, "close_pool_bounded", _close_and_signal)  # pyright: ignore[reportPrivateImportUsage]
    monkeypatch.setattr(bootstrap_mod, "close_pool_bounded", _close_and_signal)
    monkeypatch.setattr(worker_deps_mod.ReloadGate, "await_quiet", _wait_and_signal)
    try:
        await _create_run(pool, schema, compiled)
        await _create_run(pool, schema, compiled)
        captured = _capture_worker_deps(monkeypatch)
        settings = _worker_settings(
            module_pg_schema.pg_dsn, schema, tag="overlap", max_concurrency=2
        )
        connections, _pools = _real_pool_connections(module_pg_schema.pg_dsn)

        with structlog.testing.capture_logs() as logs:
            worker = asyncio.create_task(
                bootstrap_mod._main(  # pyright: ignore[reportPrivateUsage]  # Why: the real worker composition owns the reload/claim interleaving under test.
                    settings,
                    actor_registry={},
                    connections=connections,
                    until_idle=True,
                    idle_settle_window=0.2,
                    idle_poll_interval=0.1,
                    idle_max_runtime=120.0,
                )
            )
            await asyncio.wait_for(old_started.wait(), timeout=60.0)
            boot_worker_pool = captured[0].worker_pool
            boot_dispatcher_pool = captured[0].dispatcher_pool
            _GENERATION[0] = 1
            captured[0].request_reload()

            # The second claim starts while generation 0 is still draining. At
            # this point it necessarily holds generation 1 and the OLD pool.
            await asyncio.wait_for(new_started.wait(), timeout=60.0)
            assert captured[0].worker_pool is boot_worker_pool
            release_old.set()
            await asyncio.wait_for(old_pool_closed.wait(), timeout=30.0)
            await asyncio.wait_for(dispatcher_wait_started.wait(), timeout=30.0)
            assert not old_dispatcher_closed.is_set()
            assert boot_dispatcher_pool is not None
            assert not boot_dispatcher_pool.is_closing(), (
                "the old dispatcher closed while its post-rotation claim was still live"
            )
            release_new.set()
            await asyncio.wait_for(old_dispatcher_closed.wait(), timeout=30.0)
            await wait_for_condition(
                lambda: captured[0].active_jobs.count() == 0,
                description="both overlapping claims unwound",
                timeout=60.0,
            )
            assert body_errors == [], [f"{type(exc).__name__}: {exc}" for exc in body_errors]
            _assert_no_loop_or_pool_errors(logs)
            exit_code = await asyncio.wait_for(worker, timeout=120.0)

        assert exit_code == 0, exit_code
        assert _PRODUCED[0] == 2
        assert [cast("_Probe", instance).generation for instance in _INSTANCES] == [0, 1]
    finally:
        release_old.set()
        release_new.set()
        if worker is not None and not worker.done():
            worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)
        await pool.close()


@pytest.mark.integration
async def test_the_reload_pool_gate_covers_a_detached_deadline_unwind(
    module_pg_schema: ModulePgSchema,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A deadline can finalize the claim while its body task continues in
    ``finally``. The pool gate must follow that detached task too: the body's
    pool query remains live user work after the dispatch execution returned."""
    schema = module_pg_schema.schema_name
    unwinding = asyncio.Event()
    finish_unwind = asyncio.Event()
    body_done = asyncio.Event()
    body_tasks: list[asyncio.Task[object]] = []
    body_errors: list[Exception] = []

    async def _deadline_body(ctx: StepContext, doc: _Doc, deps: _Probe) -> _Report:
        del doc
        _INSTANCES.append(deps)
        try:
            await asyncio.sleep(30.0)
        finally:
            task = ctx._actor_body_task  # pyright: ignore[reportPrivateUsage]  # Why: the detached task is the exact lifetime the pool gate must protect and this pin must reap.
            assert task is not None
            body_tasks.append(task)
            unwinding.set()
            await finish_unwind.wait()
            try:
                async with ctx._pool.acquire() as conn:  # pyright: ignore[reportPrivateUsage]  # Why: the old dispatcher pool is the resource whose lifetime this integration pin proves.
                    assert await conn.fetchval("SELECT 1") == 1
            except Exception as exc:
                body_errors.append(exc)
                raise
            finally:
                body_done.set()
        return _Report(ref="unreachable")

    app = WorkflowApp(deps_factory=_make_probe, deps_generation=_declared_generation)

    @app.workflow("wf-reload-detached-pool-unwind")
    def _flow() -> Promise[object]:
        return build(
            step(
                _deadline_body,
                _Doc(doc_id="deadline-pool"),
                key="solo",
                max_attempts=1,
                start_to_close=timedelta(seconds=0.05),
            )
        )

    compiled = app.get("wf-reload-detached-pool-unwind")
    pool = await asyncpg.create_pool(module_pg_schema.pg_dsn)
    worker: asyncio.Task[int] | None = None
    old_worker_closed = asyncio.Event()
    old_dispatcher_closed = asyncio.Event()
    boot_worker_pool: asyncpg.Pool | None = None
    boot_dispatcher_pool: asyncpg.Pool | None = None
    real_reload = worker_deps_mod.reload_credentials

    async def _tight_reload(deps: Any, **kwargs: Any) -> Any:
        return await real_reload(deps, drain_timeout=0.05, **kwargs)

    monkeypatch.setattr(worker_deps_mod, "reload_credentials", _tight_reload)
    real_close = worker_deps_mod.close_pool_bounded  # pyright: ignore[reportPrivateImportUsage]  # Why: the production close site reads this module global at call time.

    async def _close_and_signal(
        closing: asyncpg.Pool, label: str, close_timeout: float, *, family: Any = "teardown"
    ) -> None:
        await real_close(closing, label, close_timeout, family=family)
        if closing is boot_worker_pool and label == "worker":
            old_worker_closed.set()
        if closing is boot_dispatcher_pool and label == "dispatcher":
            old_dispatcher_closed.set()

    monkeypatch.setattr(worker_deps_mod, "close_pool_bounded", _close_and_signal)  # pyright: ignore[reportPrivateImportUsage]
    monkeypatch.setattr(bootstrap_mod, "close_pool_bounded", _close_and_signal)
    try:
        await _create_run(pool, schema, compiled)
        captured = _capture_worker_deps(monkeypatch)
        settings = _worker_settings(
            module_pg_schema.pg_dsn, schema, tag="detached-pool", max_concurrency=1
        )
        connections, _pools = _real_pool_connections(module_pg_schema.pg_dsn)
        worker = asyncio.create_task(
            bootstrap_mod._main(  # pyright: ignore[reportPrivateUsage]  # Why: the real worker composition owns the dispatch/gate/detached-task interleaving under test.
                settings,
                actor_registry={},
                connections=connections,
            )
        )
        await asyncio.wait_for(unwinding.wait(), timeout=60.0)
        await wait_for_condition(
            lambda: captured[0].active_jobs.count() == 0,
            description="the deadline attempt finalized while its body stayed detached",
            timeout=30.0,
        )
        boot_worker_pool = captured[0].worker_pool
        boot_dispatcher_pool = captured[0].dispatcher_pool
        _GENERATION[0] = 1
        captured[0].request_reload()

        # The unheld worker pool closing proves the reload reached its close
        # phase. The dispatcher must remain open for the detached body.
        await asyncio.wait_for(old_worker_closed.wait(), timeout=30.0)
        assert not old_dispatcher_closed.is_set()
        assert boot_dispatcher_pool is not None
        assert not boot_dispatcher_pool.is_closing(), (
            "the old dispatcher closed while the detached body's finally still used it"
        )

        finish_unwind.set()
        await asyncio.wait_for(body_done.wait(), timeout=30.0)
        await asyncio.gather(*body_tasks, return_exceptions=True)
        assert body_errors == [], [f"{type(exc).__name__}: {exc}" for exc in body_errors]
        await asyncio.wait_for(old_dispatcher_closed.wait(), timeout=30.0)
    finally:
        finish_unwind.set()
        if body_tasks:
            await asyncio.gather(*body_tasks, return_exceptions=True)
        if worker is not None and not worker.done():
            worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)
        await pool.close()

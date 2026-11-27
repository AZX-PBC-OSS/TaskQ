"""The WORKER-AND-LOOP SCOPE of a workflow app's ``deps_factory`` binding.

``WorkflowApp(deps_factory=…)`` declares ONE factory per app. The worker-hosted
door executes many workflows of that app on ONE worker and ONE event loop, and
the app's own semantics are one instance per app — a static ``deps=`` instance
is shared by every workflow of the app, so the lazy arm must be shared too.
This module is the scope that makes that true inside a long-running worker:

* a resolution lives per (declared binding, worker identity, event loop), so a
  second worker, a second loop, or another app's factory gets its own instance,
  and one worker's shutdown never reaches a co-resident worker's entries;
* it is created at the first body that declares the deps parameter (the runner
  asks through the same ``DepsResolution`` seam the direct door uses) on the
  running loop, and reused for the worker's lifetime — the factory runs once
  however many bodies, runs and workflows of the app execute;
* it is torn down exactly once, bounded, by the worker bootstrap's exit stack
  AFTER the worker's TaskGroup drained and BEFORE ``open_worker_deps`` closes
  the role pools — the loop, the pools and the loop-scope connection are all
  still open while the factory-created resource releases.

The entry carries the factory GENERATION (``INITIAL_DEPS_GENERATION`` at first
use): every resolve/teardown log event and counter names it, and the entry's
:meth:`WorkerDepsResolution.rotate` advances it when a reload asks — the
superseded generation's in-flight holds are drained first (bounded), then its
instance released exactly once, before the caller's old PG pools close.

TELEMETRY. ``workflow-deps-resolved`` / ``workflow-deps-torn-down`` /
``workflow-deps-rotated`` / ``workflow-deps-drain-timeout`` /
``workflow-deps-resolve-failed`` / ``workflow-deps-teardown-*`` log events carry
``workflow``, ``worker_id`` and ``generation``; the two counters
(``taskq.workflows.deps_resolved`` / ``taskq.workflows.deps_torn_down``) carry
``workflow``, ``generation`` and the closed ``outcome`` enum only —
``worker_id`` is an identity and never a metric dimension (the cardinality
contract in ``taskq.obs._otel``), so per-worker attribution is the log line's
job.

A hard-killed worker (SIGKILL, OOM) runs no teardown at all: its loop is never
unwound, the bucket dies with the loop, and the process exit is the release —
the same bound every other worker-scoped resource has.
"""

from __future__ import annotations

import asyncio
import contextlib
import weakref
from collections.abc import AsyncGenerator
from typing import Final, cast
from uuid import UUID

import structlog

from taskq._close import CLOSE_TIMEOUT_SECS
from taskq.obs import (
    get_logger,
    record_workflow_deps_resolved,
    record_workflow_deps_torn_down,
    safe_exception_message,
    safe_type_name,
)
from taskq.workflows.api._deps_factory import (
    INITIAL_DEPS_GENERATION,
    DepsFactory,
    DepsRelease,
    DepsResolver,
    DepsRotation,
    Generation,
)

__all__ = [
    "INITIAL_DEPS_GENERATION",
    "WorkerDepsResolution",
    "prepare_worker_deps",
    "reset_worker_deps_for_tests",
    "rotate_worker_deps",
    "teardown_worker_deps",
]

logger: structlog.stdlib.BoundLogger = get_logger(__name__)

#: One teardown's bound: the worker's bounded-close budget (a close the backend
#: never completes must not wedge the worker's exit stack, exactly like the
#: pool/connection/redis closes it sits between).
_TEARDOWN_TIMEOUT_SECS: Final[float] = CLOSE_TIMEOUT_SECS


class WorkerDepsResolution:
    """One worker-and-loop-scoped deps resolution — the ``DepsResolution`` the
    worker-hosted runners drive.

    It owns the telemetry around a shared :class:`DepsResolver` and nothing
    else: the single-initialization lock, the instance, the in-flight holds and
    the one teardown attempt per generation stay the resolver's. Concurrent
    first claims from different bodies and different workflows of the same app
    serialize on that lock, so the factory runs once and every body receives
    the same instance; a reload's rotation swaps the generation and releases
    the superseded instance once its bodies have left.
    """

    def __init__(
        self,
        binding: DepsFactory,
        *,
        workflow: str,
        worker_id: UUID,
    ) -> None:
        self._resolver = DepsResolver(binding)
        #: the first claim's workflow until the first actual resolution
        #: replaces it with the deps-declaring body's workflow. The app's one
        #: instance serves every workflow, so that trigger labels both halves.
        self._workflow = workflow
        self._worker_id = worker_id
        #: generations whose current instances have reported resolution. Old
        #: and new first resolutions can overlap during a rotation, so one
        #: scalar "last reported" value would suppress or duplicate events.
        #: Rotation discards the superseded generation, allowing a later
        #: generation cycle back to that value to report its new instance.
        self._reported: set[Generation] = set()
        self._closed = False

    @property
    def binding(self) -> DepsFactory:
        return self._resolver.binding

    @property
    def generation(self) -> Generation:
        return self._resolver.generation

    @contextlib.asynccontextmanager
    async def body(self, *, workflow: str | None = None) -> AsyncGenerator[object, None]:
        """The instance the first declaring body resolves, LEASED for that
        body invocation and reused by every later one.

        *workflow* is the runner's resolving workflow. The registry entry may
        have been created by an earlier no-deps claim of another workflow in
        the same app, so the first actual resolution adopts this label.

        A failed initialization propagates unchanged (the runner's ladder owns
        the attempt's disposition) after one structured error event carrying
        the cause; nothing is cached, so the next claim retries the factory.

        The lease is what a reload's drain counts: the instance is not released
        while a body that received it is still running. Resolution and lease
        are one step, so a rotation landing mid-claim either waits for this
        body or answers the claim from the new generation.
        """
        resolving_workflow = self._workflow if workflow is None else workflow
        acquired_generation = self.generation

        def _note_acquisition(generation: Generation) -> None:
            nonlocal acquired_generation
            acquired_generation = generation

        async with contextlib.AsyncExitStack() as lease:
            try:
                instance = await lease.enter_async_context(
                    self._resolver.body(on_acquire=_note_acquisition)
                )
            except Exception as exc:
                self._log_resolve_failed(resolving_workflow, exc, acquired_generation)
                raise
            self._report_resolved(resolving_workflow, acquired_generation)
            yield instance

    @contextlib.asynccontextmanager
    async def hold(self) -> AsyncGenerator[None, None]:
        """Hold this worker-scoped resolution's current generation for one
        claim's lifetime (see :meth:`DepsResolver.hold`). No telemetry: the
        claim is not a resolution, and a drain that had to wait for it says so
        on the drain-timeout event."""
        async with self._resolver.hold():
            yield

    async def rotate(self, *, drain_timeout: float) -> DepsRotation:
        """Settle the binding's live claims and advance to its current
        generation, releasing the superseded instance once they have left.

        Called by the worker's reload hand-off (``rotate_worker_deps``) before
        ``reload_credentials`` swaps a single pool: an unchanged generation
        still drains the live one (``rotated=False``) because this is the
        reload's settle point, not only a generation change. Reports the
        transition and every failure it owns: a generation source that raised
        (the binding keeps serving its current instance — a broken settings read
        must not wedge the reload or the worker), a drain bound that expired
        (the stuck claim's clean-completion protection ends there), a release
        that timed out, and a release that raised. The worker POOL a claim
        executes on is the reload gate's lifetime, not this call's.
        """
        try:
            report = await self._resolver.rotate(
                drain_timeout=drain_timeout, release_timeout=_TEARDOWN_TIMEOUT_SECS
            )
        except Exception as exc:
            report = DepsRotation(
                rotated=False,
                generation=self.generation,
                previous_generation=self.generation,
                had_instance=self.generation in self._reported,
                drained=True,
                error=exc,
            )
        if self._closed:
            # Shutdown owns the lifecycle report for every entry it is
            # releasing. The rotation still returns what it committed, but
            # must not report that same physical release a second time.
            return report
        if not report.rotated:
            if report.error is not None:
                # The generation could not be read: nothing rotated, the
                # current instance keeps serving, and the next reload retries.
                # Reported rather than raised so a sibling binding still rotates.
                logger.error(
                    "workflow-deps-rotate-failed",
                    workflow=self._workflow,
                    worker_id=str(self._worker_id),
                    generation=self.generation,
                    error_class=safe_type_name(report.error),
                    error=safe_exception_message(report.error),
                    exc_info=report.error,
                )
            elif not report.drained:
                # A reload whose generation did not change still asks in-flight
                # claims to settle before its caller swaps the pools they hold;
                # the bound expiring is the same force-proceed contract.
                logger.warning(
                    "workflow-deps-drain-timeout",
                    workflow=self._workflow,
                    worker_id=str(self._worker_id),
                    generation=report.generation,
                    drain_timeout=drain_timeout,
                )
            return report
        self._reported.discard(report.previous_generation)
        if not report.drained:
            logger.warning(
                "workflow-deps-drain-timeout",
                workflow=self._workflow,
                worker_id=str(self._worker_id),
                generation=report.previous_generation,
                drain_timeout=drain_timeout,
            )
        if report.had_instance:
            self._report_teardown(report)
        logger.info(
            "workflow-deps-rotated",
            workflow=self._workflow,
            worker_id=str(self._worker_id),
            generation=report.generation,
            previous_generation=report.previous_generation,
            drained=report.drained,
        )
        return report

    async def aclose(self) -> None:
        """Release every instance this resolution still holds — the current
        generation plus any superseded generation a cancelled rotation left —
        each with its own bounded attempt and its own lifecycle report.

        The bound covers BOTH waits per generation — that generation's lock (a
        body still inside a hung factory holds it) and the release itself — so
        a stuck factory cannot wedge shutdown. A generation that never
        resolved released nothing: it is still closed so a late body cannot
        resolve against torn-down worker state, and it reports no teardown. A
        failure is logged with its cause and counted against the generation it
        happened on, and the remaining generations and the role pools still
        close.
        """
        self._closed = True
        for release in await self._resolver.close_generations(
            release_timeout=_TEARDOWN_TIMEOUT_SECS
        ):
            self._report_release(release)

    def _report_release(self, release: DepsRelease) -> None:
        """One generation's teardown attempt: clean, timed out, or failed —
        named by the generation that minted the instance."""
        generation = release.generation
        if release.error is not None:
            if isinstance(release.error, TimeoutError):
                logger.warning(
                    "workflow-deps-teardown-timeout",
                    workflow=self._workflow,
                    worker_id=str(self._worker_id),
                    generation=generation,
                    close_timeout=_TEARDOWN_TIMEOUT_SECS,
                )
            else:
                logger.error(
                    "workflow-deps-teardown-error",
                    workflow=self._workflow,
                    worker_id=str(self._worker_id),
                    generation=generation,
                    error_class=safe_type_name(release.error),
                    error=safe_exception_message(release.error),
                    exc_info=release.error,
                )
            if release.had_instance:
                record_workflow_deps_torn_down(self._workflow, generation, failed=True)
            return
        if not release.had_instance:
            return  # nothing was ever minted here, so there is no release to report
        logger.info(
            "workflow-deps-torn-down",
            workflow=self._workflow,
            worker_id=str(self._worker_id),
            generation=generation,
        )
        record_workflow_deps_torn_down(self._workflow, generation, failed=False)

    def _report_resolved(self, workflow: str, generation: Generation) -> None:
        """Report the acquired generation's first resolution (and adopt its
        workflow label). Overlapping old/new acquisitions each report once;
        a rotated-in generation reports itself on its own first use."""
        if generation in self._reported:
            return
        self._reported.add(generation)
        self._workflow = workflow
        logger.info(
            "workflow-deps-resolved",
            workflow=workflow,
            worker_id=str(self._worker_id),
            generation=generation,
        )
        record_workflow_deps_resolved(workflow, generation)

    def _log_resolve_failed(
        self, workflow: str, exc: BaseException, generation: Generation
    ) -> None:
        logger.error(
            "workflow-deps-resolve-failed",
            workflow=workflow,
            worker_id=str(self._worker_id),
            generation=generation,
            error_class=safe_type_name(exc),
            error=safe_exception_message(exc),
            exc_info=exc,
        )

    def _report_teardown(self, report: DepsRotation) -> None:
        """Report the superseded generation's release: clean, timed out, or
        failed with its cause."""
        error = report.error
        if error is None:
            logger.info(
                "workflow-deps-torn-down",
                workflow=self._workflow,
                worker_id=str(self._worker_id),
                generation=report.previous_generation,
            )
            record_workflow_deps_torn_down(self._workflow, report.previous_generation, failed=False)
            return
        if isinstance(error, TimeoutError):
            logger.warning(
                "workflow-deps-teardown-timeout",
                workflow=self._workflow,
                worker_id=str(self._worker_id),
                generation=report.previous_generation,
                close_timeout=_TEARDOWN_TIMEOUT_SECS,
            )
        else:
            logger.error(
                "workflow-deps-teardown-error",
                workflow=self._workflow,
                worker_id=str(self._worker_id),
                generation=report.previous_generation,
                error_class=safe_type_name(error),
                error=safe_exception_message(error),
                exc_info=error,
            )
        record_workflow_deps_torn_down(self._workflow, report.previous_generation, failed=True)


type _Key = tuple[UUID, int]
"""``(worker identity, id(binding))`` — the binding object is retained by the
resolution, so its id stays unique for as long as the entry lives."""

type _Bucket = dict[_Key, WorkerDepsResolution]
_BUCKET_ATTR: Final[str] = "_taskq_workflow_worker_deps"
_loops: weakref.WeakSet[asyncio.AbstractEventLoop] = weakref.WeakSet()
"""Loops with a resolution bucket, weakly tracked for the test reset.

The bucket itself belongs to its loop (``_BUCKET_ATTR``), not to a module-level
``WeakKeyDictionary`` value. A loop-bound resource and its asyncpg pool retain
their loop; a weak map with a strong bucket value would therefore keep its own
key alive forever. Loop ownership forms only a collectable cycle when the host
drops the loop, while this weak set never pins it.
"""


def _bucket(loop: asyncio.AbstractEventLoop) -> _Bucket | None:
    bucket = getattr(loop, _BUCKET_ATTR, None)
    return cast("_Bucket", bucket) if isinstance(bucket, dict) else None


def _bucket_for_resolution(loop: asyncio.AbstractEventLoop) -> _Bucket:
    bucket = _bucket(loop)
    if bucket is None:
        bucket = {}
        setattr(loop, _BUCKET_ATTR, bucket)
        _loops.add(loop)
    return bucket


def prepare_worker_deps(
    binding: DepsFactory,
    *,
    workflow: str,
    worker_id: UUID,
) -> WorkerDepsResolution:
    """The RUNNING loop's resolution of *binding* for *worker_id*.

    Creates the entry on first use and answers the same object afterwards, so
    every body of every workflow of the app on this worker and loop resolves
    through one factory invocation. *workflow* labels the telemetry (see
    :class:`WorkerDepsResolution`).

    Must be called on the loop that will execute the bodies: the loop is the
    entry's scope, never an ambient one.
    """
    loop = asyncio.get_running_loop()
    bucket = _bucket_for_resolution(loop)
    key: _Key = (worker_id, id(binding))
    resolution = bucket.get(key)
    if resolution is not None:
        return resolution
    resolution = WorkerDepsResolution(
        binding,
        workflow=workflow,
        worker_id=worker_id,
    )
    bucket[key] = resolution
    return resolution


async def rotate_worker_deps(worker_id: UUID, *, drain_timeout: float) -> list[DepsRotation]:
    """Settle and rotate every resolution *worker_id* holds on the RUNNING
    loop to its binding's current generation, releasing each superseded
    instance once its in-flight holds left.

    Called by the worker's reload hand-off
    (``taskq.workflows._worker_execution.reload_worker_workflow_state``) BEFORE
    the reload swaps the worker's PG pools, so a superseded deps instance is
    always released while the old pools are still open. A binding whose
    generation did not change still drains its live claims (`rotated=False`);
    a co-resident worker's resolutions are untouched.
    """
    loop = asyncio.get_running_loop()
    bucket = _bucket(loop)
    if bucket is None:
        return []
    reports: list[DepsRotation] = []
    for key in [key for key in bucket if key[0] == worker_id]:
        reports.append(await bucket[key].rotate(drain_timeout=drain_timeout))
    return reports


async def teardown_worker_deps(worker_id: UUID) -> None:
    """Release every resolution *worker_id* minted on the RUNNING loop, and
    evict them.

    Called by the worker bootstrap's exit stack after the TaskGroup drained and
    before the role pools close (``taskq.workflows._worker_execution.
    teardown_worker_workflow_state`` is that seam). One failing release is
    logged with its cause and the remaining resolutions still release, so the
    caller always gets control back and the pools still close.
    """
    loop = asyncio.get_running_loop()
    bucket = _bucket(loop)
    if bucket is None:
        return
    for key in [key for key in bucket if key[0] == worker_id]:
        resolution = bucket.pop(key)
        await resolution.aclose()
    # A co-resident worker's teardown may have detached this bucket while
    # one of this worker's releases was awaiting, and a live resolution may
    # already have installed a new bucket on the loop. Detach only the exact
    # empty bucket this call drained, never its replacement (or a second time).
    if not bucket and _bucket(loop) is bucket:
        delattr(loop, _BUCKET_ATTR)
        _loops.discard(loop)


def reset_worker_deps_for_tests() -> None:
    """Drop every entry WITHOUT releasing it — the suite's isolation seam
    (``_worker_execution.reset_app_registry_for_tests`` drives it around every
    test, beside the app registry itself).

    A test's app declarations, worker ids and loops are its own; a later test
    in the same process must not inherit a resolution minted against a binding
    whose app is gone. Never a production path: the production release is
    :func:`teardown_worker_deps`.
    """
    for loop in list(_loops):
        bucket = _bucket(loop)
        if bucket is not None:
            bucket.clear()
            delattr(loop, _BUCKET_ATTR)
    _loops.clear()

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
use; reload rotation advances it in the reload work) so resolution and teardown
telemetry name the generation that minted the instance without the schema
changing when rotation arrives.

TELEMETRY. ``workflow-deps-resolved`` / ``workflow-deps-torn-down`` /
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
import weakref
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
from taskq.workflows.api._deps_factory import DepsFactory, DepsResolver

__all__ = [
    "INITIAL_DEPS_GENERATION",
    "WorkerDepsResolution",
    "prepare_worker_deps",
    "reset_worker_deps_for_tests",
    "teardown_worker_deps",
]

logger: structlog.stdlib.BoundLogger = get_logger(__name__)

#: The generation a binding resolves at its first use. A reload that rotates
#: the factory advances it; the value rides the entry, the log events and the
#: counters so the schema is stable across that change.
INITIAL_DEPS_GENERATION: Final[int] = 0

#: One teardown's bound: the worker's bounded-close budget (a close the backend
#: never completes must not wedge the worker's exit stack, exactly like the
#: pool/connection/redis closes it sits between).
_TEARDOWN_TIMEOUT_SECS: Final[float] = CLOSE_TIMEOUT_SECS


class WorkerDepsResolution:
    """One worker-and-loop-scoped deps resolution — the ``DepsResolution`` the
    worker-hosted runners drive.

    It owns the telemetry around a shared :class:`DepsResolver` and nothing
    else: the single-initialization lock, the instance and the one teardown
    attempt stay the resolver's. Concurrent first claims from different bodies
    and different workflows of the same app serialize on that lock, so the
    factory runs once and every body receives the same instance.
    """

    def __init__(
        self,
        binding: DepsFactory,
        *,
        workflow: str,
        worker_id: UUID,
        generation: int,
    ) -> None:
        self._resolver = DepsResolver(binding)
        #: the first claim's workflow until the first actual resolution
        #: replaces it with the deps-declaring body's workflow. The app's one
        #: instance serves every workflow, so that trigger labels both halves.
        self._workflow = workflow
        self._worker_id = worker_id
        self._generation = generation
        self._resolved = False

    @property
    def binding(self) -> DepsFactory:
        return self._resolver.binding

    @property
    def generation(self) -> int:
        return self._generation

    async def get(self, *, workflow: str | None = None) -> object:
        """The instance the first declaring body resolves, reused thereafter.

        *workflow* is the runner's resolving workflow. The registry entry may
        have been created by an earlier no-deps claim of another workflow in
        the same app, so the first actual resolution adopts this label.

        A failed initialization propagates unchanged (the runner's ladder owns
        the attempt's disposition) after one structured error event carrying
        the cause; nothing is cached, so the next claim retries the factory.
        """
        resolving_workflow = self._workflow if workflow is None else workflow
        try:
            instance = await self._resolver.get()
        except Exception as exc:
            logger.error(
                "workflow-deps-resolve-failed",
                workflow=resolving_workflow,
                worker_id=str(self._worker_id),
                generation=self.generation,
                error_class=safe_type_name(exc),
                error=safe_exception_message(exc),
                exc_info=exc,
            )
            raise
        if not self._resolved:
            # No await between the test and the set: on one loop the first
            # successful resolution is the only one that reports.
            self._resolved = True
            self._workflow = resolving_workflow
            logger.info(
                "workflow-deps-resolved",
                workflow=self._workflow,
                worker_id=str(self._worker_id),
                generation=self.generation,
            )
            record_workflow_deps_resolved(self._workflow, self.generation)
        return instance

    async def aclose(self) -> None:
        """Release the instance this resolution minted: one attempt, bounded,
        never raising (the worker's exit stack keeps unwinding).

        The bound covers BOTH waits — the resolution's own lock (a body still
        inside a hung factory holds it) and the release itself — so a stuck
        factory cannot wedge shutdown. A resolution that never resolved
        releases nothing and reports no teardown; it is still closed so a late
        body cannot resolve against torn-down worker state. A failure is
        logged with its cause and the counter records the failed outcome, and
        the remaining resolutions and the role pools still close.
        """
        try:
            await asyncio.wait_for(self._resolver.aclose(), timeout=_TEARDOWN_TIMEOUT_SECS)
        except TimeoutError:
            logger.warning(
                "workflow-deps-teardown-timeout",
                workflow=self._workflow,
                worker_id=str(self._worker_id),
                generation=self.generation,
                close_timeout=_TEARDOWN_TIMEOUT_SECS,
            )
            if self._resolved:
                record_workflow_deps_torn_down(self._workflow, self.generation, failed=True)
            return
        except Exception as exc:
            logger.error(
                "workflow-deps-teardown-error",
                workflow=self._workflow,
                worker_id=str(self._worker_id),
                generation=self.generation,
                error_class=safe_type_name(exc),
                error=safe_exception_message(exc),
                exc_info=exc,
            )
            if self._resolved:
                record_workflow_deps_torn_down(self._workflow, self.generation, failed=True)
            return
        if not self._resolved:
            return  # nothing was ever minted here, so there is no release to report
        logger.info(
            "workflow-deps-torn-down",
            workflow=self._workflow,
            worker_id=str(self._worker_id),
            generation=self.generation,
        )
        record_workflow_deps_torn_down(self._workflow, self.generation, failed=False)


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
        generation=INITIAL_DEPS_GENERATION,
    )
    bucket[key] = resolution
    return resolution


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

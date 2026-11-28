"""Adversarial lifecycle probes for workflow dependency factories."""

from __future__ import annotations

import asyncio

import pytest
import structlog

import taskq.workflows.api._deps_factory as deps_factory_mod
from taskq._ids import new_uuid
from taskq.workflows._worker_deps import WorkerDepsResolution
from taskq.workflows.api._deps_factory import DepsFactory, DepsRelease, DepsResolver


async def test_cancelled_close_retains_generations_it_has_not_attempted() -> None:
    """Cancelling one bounded release must not orphan its never-attempted siblings.

    Two cancelled rotations leave two superseded generations pending. If the
    first close is itself cancelled, a later close still owns every instance
    whose teardown has not yet been attempted.
    """
    generation = 0
    generation_read = asyncio.Event()
    first_release_started = asyncio.Event()
    attempts: list[int] = []

    class Resource:
        def __init__(self, version: int) -> None:
            self.version = version

        async def aclose(self) -> None:
            attempts.append(self.version)
            if self.version == 0:
                first_release_started.set()
                await asyncio.Event().wait()

    def current_generation() -> int:
        generation_read.set()
        return generation

    def factory() -> Resource:
        return Resource(generation)

    resolver = DepsResolver(DepsFactory(factory, generation=current_generation))

    async def resolve() -> None:
        async with resolver.body():
            pass

    async def cancel_rotation() -> None:
        nonlocal generation
        lease = resolver.body()
        await lease.__aenter__()
        try:
            generation += 1
            generation_read.clear()
            rotation = asyncio.create_task(resolver.rotate(drain_timeout=30.0))
            await asyncio.wait_for(generation_read.wait(), timeout=5.0)
            rotation.cancel()
            with pytest.raises(asyncio.CancelledError):
                await rotation
        finally:
            await lease.__aexit__(None, None, None)
        await resolve()

    await resolve()
    await cancel_rotation()
    await cancel_rotation()

    close = asyncio.create_task(resolver.aclose())
    await asyncio.wait_for(first_release_started.wait(), timeout=5.0)
    close.cancel()
    with pytest.raises(asyncio.CancelledError):
        await close

    await resolver.aclose()

    assert attempts == [0, 1, 2], (
        "a cancelled close forgot a pending generation it had never attempted"
    )


async def test_a_late_acquisition_release_is_bounded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A resource arriving after its owner's bounded close still gets a
    bounded release, rather than parking the claim forever in a late close."""
    monkeypatch.setattr(deps_factory_mod, "CLOSE_TIMEOUT_SECS", 0.05)
    factory_entered = asyncio.Event()
    finish_factory = asyncio.Event()
    release_started = asyncio.Event()
    finish_release = asyncio.Event()
    release_attempts: list[int] = []

    class Resource:
        async def aclose(self) -> None:
            release_attempts.append(1)
            release_started.set()
            await finish_release.wait()

    async def factory() -> Resource:
        factory_entered.set()
        await finish_factory.wait()
        return Resource()

    resolver = DepsResolver(DepsFactory(factory))

    async def claim() -> None:
        async with resolver.body():
            pass

    claim_task = asyncio.create_task(claim())
    try:
        await asyncio.wait_for(factory_entered.wait(), timeout=1.0)
        releases = await resolver.close_generations(release_timeout=0.05)
        assert len(releases) == 1 and isinstance(releases[0].error, TimeoutError)

        finish_factory.set()
        await asyncio.wait_for(release_started.wait(), timeout=1.0)
        with pytest.raises(RuntimeError, match="deps owner is closed"):
            await asyncio.wait_for(claim_task, timeout=0.2)
    finally:
        finish_factory.set()
        finish_release.set()
        await asyncio.gather(claim_task, return_exceptions=True)

    assert release_attempts == [1]


async def test_shutdown_during_a_rotations_release_still_reports_that_release(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A shutdown arriving after a rotation entered the superseded release
    must not suppress both owners' lifecycle report for that one release."""
    generation = 0
    release_started = asyncio.Event()
    finish_release = asyncio.Event()
    shutdown_started = asyncio.Event()
    releases: list[int] = []

    class Resource:
        def __init__(self, version: int) -> None:
            self.version = version

    async def factory() -> Resource:
        return Resource(generation)

    async def teardown(instance: object) -> None:
        resource = instance
        assert isinstance(resource, Resource)
        releases.append(resource.version)
        if resource.version == 0:
            release_started.set()
            await finish_release.wait()

    resolution = WorkerDepsResolution(
        DepsFactory(factory, teardown=teardown, generation=lambda: generation),
        workflow="wf-overlap-release",
        worker_id=new_uuid(),
    )
    async with resolution.body():
        pass

    resolver = resolution._resolver  # pyright: ignore[reportPrivateUsage]  # Why: the close-call observation is the deterministic boundary for this overlap probe.
    real_close_generations = resolver.close_generations

    async def _observed_close_generations(*, release_timeout: float) -> list[DepsRelease]:
        shutdown_started.set()
        return await real_close_generations(release_timeout=release_timeout)

    monkeypatch.setattr(resolver, "close_generations", _observed_close_generations)

    generation = 1
    with structlog.testing.capture_logs() as logs:
        rotation = asyncio.create_task(resolution.rotate(drain_timeout=1.0))
        await asyncio.wait_for(release_started.wait(), timeout=1.0)
        shutdown = asyncio.create_task(resolution.aclose())
        await asyncio.wait_for(shutdown_started.wait(), timeout=1.0)
        finish_release.set()
        report = await asyncio.wait_for(rotation, timeout=1.0)
        await asyncio.wait_for(shutdown, timeout=1.0)

    assert report.rotated and report.had_instance
    assert releases == [0]
    torn_down = [entry for entry in logs if entry["event"] == "workflow-deps-torn-down"]
    assert [entry["generation"] for entry in torn_down] == [0], (
        "the physical superseded release lost its only lifecycle report"
    )


async def test_recycled_generations_keep_distinct_teardown_reports() -> None:
    """A cancelled rotation can leave an old generation pending while a later
    rotation legally returns to the same label. Two physical instances with
    that label still need two lifecycle reports."""
    generation = 0
    generation_read = asyncio.Event()
    serial = 0
    releases: list[int] = []

    class Resource:
        def __init__(self, identity: int) -> None:
            self.identity = identity

    def current_generation() -> int:
        generation_read.set()
        return generation

    async def factory() -> Resource:
        nonlocal serial
        identity = serial
        serial += 1
        return Resource(identity)

    async def teardown(instance: object) -> None:
        assert isinstance(instance, Resource)
        releases.append(instance.identity)

    resolution = WorkerDepsResolution(
        DepsFactory(factory, teardown=teardown, generation=current_generation),
        workflow="wf-recycled-generation",
        worker_id=new_uuid(),
    )

    async def resolve() -> None:
        async with resolution.body():
            pass

    with structlog.testing.capture_logs() as logs:
        await resolve()  # instance 0, generation 0
        lease = resolution.body()
        await lease.__aenter__()
        generation = 1
        rotation = asyncio.create_task(resolution.rotate(drain_timeout=30.0))
        await asyncio.wait_for(generation_read.wait(), timeout=1.0)
        rotation.cancel()
        with pytest.raises(asyncio.CancelledError):
            await rotation
        await lease.__aexit__(None, None, None)

        await resolve()  # instance 1, generation 1
        generation = 0
        report = await resolution.rotate(drain_timeout=1.0)
        assert report.rotated
        await resolve()  # instance 2, a fresh generation-0 entry
        await resolution.aclose()

    assert sorted(releases) == [0, 1, 2]
    torn_down = [entry for entry in logs if entry["event"] == "workflow-deps-torn-down"]
    assert len(torn_down) == 3, (
        "two different physical generation-0 instances must not share one report"
    )

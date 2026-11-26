"""The worker-and-loop scope of ``deps_factory`` (#719): the WORKER-hosted door.

A static ``deps=`` instance is one per app by construction. The lazy arm —
``WorkflowApp(deps_factory=…)`` — must reach the same answer inside a
long-running worker: ONE instance per app per worker per event loop, minted on
the first body that declares the deps parameter and reused by every body,
workflow and run until the worker shuts down; released exactly once, bounded,
while the worker's loop and PG pools are still open, and never on a
co-resident worker's state.

These pins drive the real doors:

* ``execute_flow_job`` (the fleet-claimed row) against the real PG fixtures —
  one instance across two workflows of one app and several runs, the lock
  serializing concurrent first claims, two workers resolving distinct
  instances, the worker teardown releasing once and evicting the runner cache;
* ``_bootstrap._main`` — the real worker process (its own dispatch claim, the
  consumer's flow intercept, the exit stack) on both exit paths the acceptance
  criteria name: a graceful shutdown and a sibling crash's TaskGroup unwind.
  The teardown's ordering against the role pools is read off the bounded-close
  tap (the ``_close_events`` seam ``test_worker_deps_teardown`` established):
  ``close_pool_bounded`` appends its pool's label, the deps release appends its
  own, and one list proves which happened first.

Telemetry pins sit beside them: structured logs naming ``workflow``,
``worker_id`` and ``generation`` on resolve and teardown, the two counters
(``taskq.workflows.deps_resolved`` / ``...deps_torn_down``) carrying the
bounded dimensions only, and both no-op-safe with OTel disabled.
"""

from __future__ import annotations

import asyncio
import gc
import json
import weakref
from collections.abc import Awaitable, Callable, Sequence
from datetime import UTC, datetime
from typing import Any, cast
from uuid import UUID

import asyncpg
import pytest
import structlog.testing
from pydantic import BaseModel

import taskq.obs._otel as otel_mod
import taskq.worker._bootstrap as bootstrap_mod
import taskq.worker.deps as worker_deps_mod
from taskq._di import ProviderRegistry, Scope
from taskq._ids import new_uuid
from taskq.backend._protocol import JobId, JobRow
from taskq.obs import set_otel_enabled
from taskq.obs._redact_exc import safe_exception_parts
from taskq.settings import WorkerSettings
from taskq.testing.assertions import wait_for_condition
from taskq.testing.fixtures import ModulePgSchema
from taskq.testing.health import unique_health_sock_path
from taskq.testing.otel import counter_data_points, setup_meter
from taskq.workflows import (
    FlowRunner,
    Promise,
    StepContext,
    WorkflowApp,
    build,
    step,
)
from taskq.workflows import _worker_deps as scope
from taskq.workflows import _worker_execution as seam
from taskq.workflows.api._deps_factory import DepsFactory

# ── the probe resource, its factories, and the observing body ────────────

_PRODUCED: list[int] = []
"""Factory invocation count (the one-instance-per-worker witness)."""

_INSTANCES: list[object] = []
"""The deps instance each body observed, in execution order."""

_EXECUTED: list[str] = []
"""One entry per body that ran."""

_ORDER: list[str] = []
"""Lifecycle markers, in the order they happened: the deps release and each
role pool's bounded close (the before-pool-close proof)."""


def _reset() -> None:
    _PRODUCED.clear()
    _PRODUCED.append(0)
    _INSTANCES.clear()
    _EXECUTED.clear()
    _ORDER.clear()


@pytest.fixture(autouse=True)
def _reset_probe_state() -> None:  # pyright: ignore[reportUnusedFunction]  # Why: autouse fixture consumed implicitly by the test runner.
    """Every test starts from an empty probe ledger (the module-level lists are
    this file's observation surface, never shared state)."""
    _reset()


class _Probe:
    """The factory-created deps resource: loop-bound, released by ``aclose()``
    (the shape a pool or an HTTP client has)."""

    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self.loop = loop

    async def aclose(self) -> None:
        _ORDER.append("deps-teardown")


class _FailingProbe(_Probe):
    """A resource whose release raises (the error-isolation pin). It still
    marks the attempt, so the ordering assertion keeps its evidence."""

    async def aclose(self) -> None:
        _ORDER.append("deps-teardown")
        try:
            raise OSError("release backend unavailable")
        except OSError as exc:
            raise RuntimeError("deps release exploded") from exc


class _HangingProbe(_Probe):
    """A resource whose release never completes (the bound pin)."""

    async def aclose(self) -> None:
        await asyncio.Event().wait()


async def _make_probe() -> _Probe:
    _PRODUCED[0] += 1
    return _Probe(asyncio.get_running_loop())


async def _make_failing_probe() -> _Probe:
    _PRODUCED[0] += 1
    return _FailingProbe(asyncio.get_running_loop())


async def _make_hanging_probe() -> _Probe:
    _PRODUCED[0] += 1
    return _HangingProbe(asyncio.get_running_loop())


async def _make_probe_slowly() -> _Probe:
    """Widens the first-claim window so a concurrent second claim really does
    enter the resolution while the factory is still running."""
    _PRODUCED[0] += 1
    for _ in range(5):
        await asyncio.sleep(0)
    return _Probe(asyncio.get_running_loop())


class _Doc(BaseModel):
    doc_id: str


class _Report(BaseModel):
    ref: str


async def _deps_body(ctx: StepContext, doc: _Doc, deps: _Probe) -> _Report:
    assert deps.loop is asyncio.get_running_loop(), "the factory did not run on this loop"
    _EXECUTED.append(doc.doc_id)
    _INSTANCES.append(deps)
    return _Report(ref=doc.doc_id)


def _declare(app: WorkflowApp, name: str) -> None:
    @app.workflow(name)
    def _build() -> Promise[object]:
        return build(
            step(_deps_body, _Doc(doc_id=name), key="solo", actor="wf-scope", queue="default")
        )


def _scope_app(
    names: Sequence[str],
    *,
    factory: Callable[[], Awaitable[_Probe]] = _make_probe,
) -> WorkflowApp:
    """One app, its workflows, and one factory binding for all of them."""
    app = WorkflowApp(deps_factory=factory)
    for name in names:
        _declare(app, name)
    return app


# ── the fleet-claimed row (the door's own read model) ────────────────────


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
        actor="wf-scope",
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


async def _create_run(pool: asyncpg.Pool, schema: str, compiled: Any, doc_id: str) -> JobId:
    """The run's rows (the root + the claimed-able node), written by the
    runner's own ``create_flow`` — the production producer."""
    async with FlowRunner(compiled, pool, schema) as runner:
        return (await runner.create_flow(input={"doc_id": doc_id})).flow_id


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


# ── the scope itself (no PG: the registry's law) ─────────────────────────


def test_shutdown_budget_counts_one_factory_binding_per_app() -> None:
    """The worst-case close tail counts each app's binding once, not once
    per workflow that shares it."""
    app = _scope_app(["wf-budget-a", "wf-budget-b"])
    app.get("wf-budget-a")
    app.get("wf-budget-b")
    other = _scope_app(["wf-budget-other"])
    other.get("wf-budget-other")

    assert seam.workflow_deps_factory_count() == 2


def test_a_resolution_is_scoped_per_binding_worker_and_event_loop() -> None:
    """The registry's key is the app's declared binding, the worker identity
    and the RUNNING loop — one entry per combination, nothing shared across
    any of the three."""
    binding = DepsFactory(_make_probe)
    other_binding = DepsFactory(_make_probe)
    worker_id = new_uuid()
    other_worker = new_uuid()

    async def _one_loop() -> tuple[object, object, object, object]:
        first = scope.prepare_worker_deps(binding, workflow="wf-a", worker_id=worker_id)
        again = scope.prepare_worker_deps(binding, workflow="wf-b", worker_id=worker_id)
        other_worker_entry = scope.prepare_worker_deps(
            binding, workflow="wf-a", worker_id=other_worker
        )
        other_binding_entry = scope.prepare_worker_deps(
            other_binding, workflow="wf-a", worker_id=worker_id
        )
        return first, again, other_worker_entry, other_binding_entry

    first, again, other_worker_entry, other_binding_entry = asyncio.run(_one_loop())
    assert again is first, "one binding, worker and loop is ONE resolution"
    assert other_worker_entry is not first, "a second worker resolves its own instance"
    assert other_binding_entry is not first, "a second app's factory resolves its own instance"

    # A fresh loop is a fresh scope, even for the same binding and worker.
    second_loop = asyncio.run(_one_loop())[0]
    assert second_loop is not first, "a second event loop must never share the instance"


def test_teardown_evicts_only_its_workers_entries() -> None:
    """One worker's teardown releases and evicts its own resolutions and
    leaves a co-resident worker's live entry untouched."""
    binding = DepsFactory(_make_probe)
    worker_id = new_uuid()
    neighbour = new_uuid()

    async def _scenario() -> None:
        mine = scope.prepare_worker_deps(binding, workflow="wf-a", worker_id=worker_id)
        theirs = scope.prepare_worker_deps(binding, workflow="wf-a", worker_id=neighbour)
        my_instance = await mine.get()
        their_instance = await theirs.get()
        assert _PRODUCED[0] == 2
        assert my_instance is not their_instance

        await scope.teardown_worker_deps(worker_id)
        assert _ORDER == ["deps-teardown"], "only the shutting-down worker's instance released"
        assert await theirs.get() is their_instance, "the neighbour still serves ITS instance"
        with pytest.raises(RuntimeError, match="deps owner is closed"):
            await mine.get()
        # re-preparing the torn-down worker's key mints a fresh entry.
        replacement = scope.prepare_worker_deps(binding, workflow="wf-a", worker_id=worker_id)
        assert replacement is not mine, "the torn-down worker's entry was evicted"
        await scope.teardown_worker_deps(neighbour)
        assert _ORDER == ["deps-teardown", "deps-teardown"]

    asyncio.run(_scenario())


def test_concurrent_teardowns_do_not_evict_a_new_live_bucket() -> None:
    """Two co-resident workers may unwind together on one loop. A teardown
    that resumed after its neighbour must not delete the loop attribute now
    owned by a newer resolution bucket."""
    first_worker, second_worker, live_worker = new_uuid(), new_uuid(), new_uuid()

    async def _scenario() -> None:
        class ControlledProbe(_Probe):
            def __init__(
                self,
                started: asyncio.Event,
                finish: asyncio.Event,
            ) -> None:
                super().__init__(asyncio.get_running_loop())
                self.started = started
                self.finish = finish

            async def aclose(self) -> None:
                self.started.set()
                await self.finish.wait()
                await super().aclose()

        first_started, second_started = asyncio.Event(), asyncio.Event()
        finish_first, finish_second = asyncio.Event(), asyncio.Event()
        first = scope.prepare_worker_deps(
            DepsFactory(lambda: ControlledProbe(first_started, finish_first)),
            workflow="wf-concurrent-first",
            worker_id=first_worker,
        )
        second = scope.prepare_worker_deps(
            DepsFactory(lambda: ControlledProbe(second_started, finish_second)),
            workflow="wf-concurrent-second",
            worker_id=second_worker,
        )
        await first.get()
        await second.get()

        first_teardown = asyncio.create_task(scope.teardown_worker_deps(first_worker))
        await first_started.wait()
        second_teardown = asyncio.create_task(scope.teardown_worker_deps(second_worker))
        await second_started.wait()
        finish_second.set()
        await second_teardown

        # The second teardown emptied and detached the old bucket. This entry
        # now lives in a NEW bucket on the same loop; the first teardown is
        # still parked in its resource's release.
        live = scope.prepare_worker_deps(
            DepsFactory(_make_probe), workflow="wf-still-live", worker_id=live_worker
        )
        await live.get()
        finish_first.set()
        await first_teardown

        assert (
            scope.prepare_worker_deps(live.binding, workflow="wf-still-live", worker_id=live_worker)
            is live
        ), "an older teardown detached the newer worker's live bucket"
        await scope.teardown_worker_deps(live_worker)
        assert _ORDER == ["deps-teardown", "deps-teardown", "deps-teardown"]

    asyncio.run(_scenario())


def test_an_abandoned_loop_bound_resolution_does_not_pin_its_loop() -> None:
    """The weak loop key must really weakly scope a loop-bound resource.

    A worker lifecycle abandoned without its teardown hook must not leave the
    whole loop reachable through registry -> resolution -> resource -> loop.
    """
    binding = DepsFactory(_make_probe)
    worker_id = new_uuid()
    loop_ref: weakref.ReferenceType[asyncio.AbstractEventLoop] | None = None

    async def _abandon() -> None:
        nonlocal loop_ref
        resolution = scope.prepare_worker_deps(
            binding, workflow="wf-abandoned-resolution", worker_id=worker_id
        )
        await resolution.get()
        loop_ref = weakref.ref(asyncio.get_running_loop())

    asyncio.run(_abandon())
    gc.collect()

    assert loop_ref is not None
    assert loop_ref() is None, "the abandoned deps registry kept its event loop alive"


def test_an_abandoned_runner_cache_does_not_pin_its_pool_loop() -> None:
    """A cached runner retains its pool, and a real asyncpg pool retains its
    loop; that value chain must not defeat the cache's weak loop key."""
    app = _scope_app(["wf-abandoned-runner"])
    app.get("wf-abandoned-runner")
    worker_id = JobId(new_uuid())
    loop_ref: weakref.ReferenceType[asyncio.AbstractEventLoop] | None = None

    async def _abandon() -> None:
        nonlocal loop_ref
        # min_size=0 builds the real pool without opening a network connection.
        pool = await asyncpg.create_pool("postgresql://localhost/postgres", min_size=0)
        assert pool is not None
        try:
            seam._runner_for(  # pyright: ignore[reportPrivateUsage]  # Why: the production runner cache is the surface whose lifetime is under test.
                "wf-abandoned-runner", pool, "scope_probe_schema", worker_id
            )
            loop_ref = weakref.ref(asyncio.get_running_loop())
        finally:
            await pool.close()

    asyncio.run(_abandon())
    gc.collect()

    assert loop_ref is not None
    assert loop_ref() is None, "the abandoned runner cache kept its pool's event loop alive"


def test_a_second_teardown_releases_nothing() -> None:
    """Teardown is idempotent: the resolved instance is released exactly once
    however often the worker's exit path asks."""
    binding = DepsFactory(_make_probe)
    worker_id = new_uuid()

    async def _scenario() -> None:
        resolution = scope.prepare_worker_deps(binding, workflow="wf-a", worker_id=worker_id)
        await resolution.get()
        await scope.teardown_worker_deps(worker_id)
        await scope.teardown_worker_deps(worker_id)
        assert _ORDER == ["deps-teardown"], _ORDER

    asyncio.run(_scenario())


def test_teardown_of_a_never_resolved_entry_reports_and_releases_nothing() -> None:
    """A worker that booted with a factory-bound app but never ran a
    deps-declaring body has nothing to release: the teardown is silent (no
    log, no counter) and still evicts the entry."""
    binding = DepsFactory(_make_probe)
    worker_id = new_uuid()

    async def _scenario() -> None:
        entry = scope.prepare_worker_deps(binding, workflow="wf-idle", worker_id=worker_id)
        with structlog.testing.capture_logs() as logs:
            await scope.teardown_worker_deps(worker_id)
        assert [e for e in logs if e["event"].startswith("workflow-deps-")] == []
        assert _PRODUCED[0] == 0, "nothing resolved, so the factory never ran"
        assert _ORDER == []
        assert (
            scope.prepare_worker_deps(binding, workflow="wf-idle", worker_id=worker_id) is not entry
        ), "the entry was evicted"

    asyncio.run(_scenario())


def test_a_failing_release_is_logged_and_the_rest_still_release() -> None:
    """One resource's failing release carries its cause into the log and does
    not stop the worker's other resolutions from releasing (the exit stack
    must reach the role pools)."""
    good = DepsFactory(_make_probe)
    bad = DepsFactory(_make_failing_probe)
    worker_id = new_uuid()

    async def _scenario() -> None:
        await scope.prepare_worker_deps(good, workflow="wf-good", worker_id=worker_id).get()
        await scope.prepare_worker_deps(bad, workflow="wf-bad", worker_id=worker_id).get()

        with structlog.testing.capture_logs() as logs:
            await scope.teardown_worker_deps(worker_id)

        assert _ORDER.count("deps-teardown") == 2, f"both releases were attempted; got {_ORDER}"
        failure = next(e for e in logs if e["event"] == "workflow-deps-teardown-error")
        assert failure["workflow"] == "wf-bad"
        assert failure["worker_id"] == str(worker_id)
        assert failure["error_class"] == "RuntimeError"
        assert failure["log_level"] == "error"
        assert "deps release exploded" in failure["error"]
        exc_info = failure.get("exc_info")
        assert isinstance(exc_info, BaseException)
        rendered = safe_exception_parts(exc_info)
        assert rendered is not None
        assert "release backend unavailable" in rendered["exception.stacktrace"]
        assert [e for e in logs if e["event"] == "workflow-deps-torn-down"], (
            "the healthy resolution's teardown still reports"
        )

    asyncio.run(_scenario())


def test_a_hanging_release_is_bounded_and_reported(monkeypatch: pytest.MonkeyPatch) -> None:
    """A release the backend never completes is bounded by the worker's
    bounded-close budget and reported — never an unbounded shutdown hang."""
    monkeypatch.setattr(scope, "_TEARDOWN_TIMEOUT_SECS", 0.05)
    binding = DepsFactory(_make_hanging_probe)
    worker_id = new_uuid()

    async def _scenario() -> None:
        await scope.prepare_worker_deps(binding, workflow="wf-hang", worker_id=worker_id).get()
        with structlog.testing.capture_logs() as logs:
            await asyncio.wait_for(scope.teardown_worker_deps(worker_id), timeout=2.0)
        timeout_event = next(e for e in logs if e["event"] == "workflow-deps-teardown-timeout")
        assert timeout_event["workflow"] == "wf-hang"
        assert timeout_event["close_timeout"] == 0.05

    asyncio.run(_scenario())


def test_teardown_is_bounded_while_a_body_is_still_inside_the_factory(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A hung factory holds the resolution's lock; teardown must still return
    inside the bounded window and report it. When the factory later answers,
    the closed owner must release that late resource rather than hand it to a
    body with no teardown owner left."""
    monkeypatch.setattr(scope, "_TEARDOWN_TIMEOUT_SECS", 0.05)
    worker_id = new_uuid()

    async def _scenario() -> None:
        entered = asyncio.Event()
        hold = asyncio.Event()

        async def _hung_factory() -> _Probe:
            _PRODUCED[0] += 1
            entered.set()
            await hold.wait()
            return _Probe(asyncio.get_running_loop())

        resolution = scope.prepare_worker_deps(
            DepsFactory(_hung_factory), workflow="wf-stuck", worker_id=worker_id
        )
        claim = asyncio.create_task(resolution.get())
        await entered.wait()

        with structlog.testing.capture_logs() as logs:
            await asyncio.wait_for(scope.teardown_worker_deps(worker_id), timeout=2.0)

        stuck = [e for e in logs if e["event"] == "workflow-deps-teardown-timeout"]
        assert len(stuck) == 1, logs
        assert stuck[0]["workflow"] == "wf-stuck"

        # Let the parked claim finish so the test leaves no task behind. The
        # late acquisition is released immediately, never handed out as live.
        hold.set()
        with pytest.raises(RuntimeError, match="deps owner is closed"):
            await claim
        assert _ORDER == ["deps-teardown"], _ORDER

    asyncio.run(_scenario())


def test_a_failing_factory_reports_its_cause_and_is_retried() -> None:
    """Initialization failure: the factory's own exception reaches the caller
    unchanged (the body's ladder owns the attempt), one structured error event
    carries the cause, and the next call retries the factory."""
    failures = [1]

    async def _flaky() -> _Probe:
        _PRODUCED[0] += 1
        if failures[0] > 0:
            failures[0] -= 1
            try:
                raise OSError("factory backend unavailable")
            except OSError as exc:
                raise RuntimeError("deps factory exploded") from exc
        return _Probe(asyncio.get_running_loop())

    worker_id = new_uuid()

    async def _scenario() -> None:
        resolution = scope.prepare_worker_deps(
            DepsFactory(_flaky), workflow="wf-flaky", worker_id=worker_id
        )
        with structlog.testing.capture_logs() as logs:
            with pytest.raises(RuntimeError, match="deps factory exploded"):
                await resolution.get()
            instance = await resolution.get()
        failure = next(e for e in logs if e["event"] == "workflow-deps-resolve-failed")
        assert (failure["workflow"], failure["worker_id"]) == ("wf-flaky", str(worker_id))
        assert failure["error_class"] == "RuntimeError"
        assert "deps factory exploded" in failure["error"]
        exc_info = failure.get("exc_info")
        assert isinstance(exc_info, BaseException)
        rendered = safe_exception_parts(exc_info)
        assert rendered is not None
        assert "factory backend unavailable" in rendered["exception.stacktrace"]
        assert [e for e in logs if e["event"] == "workflow-deps-resolved"], (
            "the successful retry reports the resolution"
        )
        assert _PRODUCED[0] == 2, "the failed attempt cached nothing"
        assert cast("_Probe", instance).loop is asyncio.get_running_loop()

    asyncio.run(_scenario())


def test_factory_failure_logs_scrub_credentials() -> None:
    """Factory errors cross the telemetry trust boundary only after the
    repository's credential mask, even when captured before log rendering."""

    async def _factory_with_dsn_error() -> _Probe:
        raise RuntimeError("cannot open postgresql://factory:fake-password@db.example/flows")

    worker_id = new_uuid()

    async def _scenario() -> None:
        resolution = scope.prepare_worker_deps(
            DepsFactory(_factory_with_dsn_error),
            workflow="wf-secret-resolve",
            worker_id=worker_id,
        )
        with structlog.testing.capture_logs() as logs, pytest.raises(RuntimeError):
            await resolution.get()
        failure = next(e for e in logs if e["event"] == "workflow-deps-resolve-failed")
        assert "fake-password" not in failure["error"]
        assert "postgresql://factory:***@db.example/flows" in failure["error"]

    asyncio.run(_scenario())


class _HostileError(RuntimeError):
    """User code can raise an exception whose own rendering also fails."""

    def __str__(self) -> str:
        raise ValueError("exception rendering exploded")


def test_a_factory_error_with_broken_str_still_propagates_unchanged() -> None:
    """Failure telemetry must not replace the factory's actual exception with
    a new rendering error; the original failure owns the attempt's verdict."""

    async def _hostile_factory() -> _Probe:
        raise _HostileError()

    worker_id = new_uuid()

    async def _scenario() -> None:
        resolution = scope.prepare_worker_deps(
            DepsFactory(_hostile_factory), workflow="wf-hostile-resolve", worker_id=worker_id
        )
        with structlog.testing.capture_logs(), pytest.raises(_HostileError):
            await resolution.get()

    asyncio.run(_scenario())


def test_a_teardown_error_with_broken_str_does_not_escape() -> None:
    """A broken exception renderer must not defeat error-isolated teardown and
    strand the remaining resolutions or role pools behind it."""

    class HostileCloseProbe(_Probe):
        async def aclose(self) -> None:
            raise _HostileError()

    async def _hostile_close_factory() -> _Probe:
        return HostileCloseProbe(asyncio.get_running_loop())

    worker_id = new_uuid()

    async def _scenario() -> None:
        resolution = scope.prepare_worker_deps(
            DepsFactory(_hostile_close_factory),
            workflow="wf-hostile-teardown",
            worker_id=worker_id,
        )
        await resolution.get()
        with structlog.testing.capture_logs() as logs:
            await scope.teardown_worker_deps(worker_id)
        failure = next(e for e in logs if e["event"] == "workflow-deps-teardown-error")
        assert failure["error"] == "<exception str() failed>"

    asyncio.run(_scenario())


# ── telemetry ────────────────────────────────────────────────────────────


async def test_resolve_and_teardown_emit_workflow_worker_and_generation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The resolve/teardown pair is provable from telemetry: structured log
    events name ``workflow``, ``worker_id`` and ``generation``, and the two
    counters carry the bounded dimensions (``workflow`` and ``generation`` —
    never the identity-like ``worker_id``)."""
    reader = setup_meter(monkeypatch)
    binding = DepsFactory(_make_probe)
    worker_id = new_uuid()
    resolution = scope.prepare_worker_deps(binding, workflow="wf-telemetry", worker_id=worker_id)

    with structlog.testing.capture_logs() as logs:
        await resolution.get()
        await resolution.get()  # reused: no second resolve event
        await scope.teardown_worker_deps(worker_id)

    resolved = [e for e in logs if e["event"] == "workflow-deps-resolved"]
    assert len(resolved) == 1, f"one resolution, one event; got {resolved}"
    assert resolved[0]["workflow"] == "wf-telemetry"
    assert resolved[0]["worker_id"] == str(worker_id)
    assert resolved[0]["generation"] == scope.INITIAL_DEPS_GENERATION

    torn = [e for e in logs if e["event"] == "workflow-deps-torn-down"]
    assert len(torn) == 1, f"one teardown, one event; got {torn}"
    assert torn[0]["workflow"] == "wf-telemetry"
    assert torn[0]["worker_id"] == str(worker_id)
    assert torn[0]["generation"] == scope.INITIAL_DEPS_GENERATION

    resolved_points = counter_data_points(reader, "taskq.workflows.deps_resolved")
    assert [(p.value, p.attributes) for p in resolved_points] == [
        (1, {"workflow": "wf-telemetry", "generation": scope.INITIAL_DEPS_GENERATION})
    ], resolved_points
    torn_points = counter_data_points(reader, "taskq.workflows.deps_torn_down")
    torn_rows: list[tuple[object, object, object, int]] = []
    for point in torn_points:
        assert point.attributes is not None
        torn_rows.append(
            (
                point.attributes["workflow"],
                point.attributes["generation"],
                point.attributes["outcome"],
                point.value,
            )
        )
    assert torn_rows == [("wf-telemetry", scope.INITIAL_DEPS_GENERATION, "ok", 1)], torn_points


def test_lifecycle_counters_do_not_propagate_otel_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The obs layer's no-raise contract covers these counters too: an SDK
    failure must not fail a resolved body or interrupt worker teardown."""

    class FailingCounter:
        def add(self, amount: int, attributes: object) -> None:
            del amount, attributes
            raise RuntimeError("metric backend exploded")

    monkeypatch.setattr(otel_mod, "_lazy_counter", lambda *args, **kwargs: FailingCounter())
    set_otel_enabled(True)

    with structlog.testing.capture_logs() as logs:
        otel_mod.record_workflow_deps_resolved("wf-metric-failure", 0)
        otel_mod.record_workflow_deps_torn_down("wf-metric-failure", 0, failed=False)

    failures = [e for e in logs if e["event"] == "otel-metric-record-failed"]
    assert [e["instrument_name"] for e in failures] == [
        "taskq.workflows.deps_resolved",
        "taskq.workflows.deps_torn_down",
    ]


def test_telemetry_is_a_noop_with_otel_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    """The counters respect the OTel switch (no instrument traffic at all),
    while the structured log events still record the lifecycle — logs are not
    an OTel signal and must survive telemetry being off."""
    reader = setup_meter(monkeypatch)
    set_otel_enabled(False)
    binding = DepsFactory(_make_probe)
    worker_id = new_uuid()

    async def _scenario() -> None:
        resolution = scope.prepare_worker_deps(binding, workflow="wf-otel-off", worker_id=worker_id)
        with structlog.testing.capture_logs() as logs:
            await resolution.get()
            await scope.teardown_worker_deps(worker_id)
        assert [e["event"] for e in logs if e["event"].startswith("workflow-deps-")] == [
            "workflow-deps-resolved",
            "workflow-deps-torn-down",
        ]

    asyncio.run(_scenario())
    assert counter_data_points(reader, "taskq.workflows.deps_resolved") == []
    assert counter_data_points(reader, "taskq.workflows.deps_torn_down") == []


# ── the runner's injected resolution ─────────────────────────────────────


def test_a_runner_drives_an_injected_resolution_without_closing_it() -> None:
    """The worker-scoped resolution is driven through the runner's own deps
    seam but owned by the worker: ``aclose()`` on a runner must never release
    an instance other bodies still share, and the worker's teardown does."""
    app = _scope_app(["wf-scope-runner"])
    compiled = app.get("wf-scope-runner")
    binding = compiled.deps_factory
    assert binding is not None
    worker_id = new_uuid()

    async def _scenario() -> None:
        resolution = scope.prepare_worker_deps(
            binding, workflow="wf-scope-runner", worker_id=worker_id
        )
        runner = FlowRunner(
            compiled,
            cast("Any", object()),
            "scope_probe_schema",
            worker_id=JobId(worker_id),
            deps_resolution=resolution,
        )
        instance = await resolution.get()
        await runner.aclose()
        assert _ORDER == [], "the runner does not own the worker's resolution"
        assert await resolution.get() is instance, "the shared instance survives the runner"

        await scope.teardown_worker_deps(worker_id)
        assert _ORDER == ["deps-teardown"], _ORDER

    asyncio.run(_scenario())


def test_injected_resolution_labels_the_workflow_that_actually_resolves() -> None:
    """An app may claim a no-deps body before another workflow's first
    deps-declaring body. Resolution telemetry names the body that actually
    minted the instance, not the earlier claim that created the empty entry."""
    app = _scope_app(["wf-scope-resolving-workflow"])
    compiled = app.get("wf-scope-resolving-workflow")
    binding = compiled.deps_factory
    assert binding is not None
    worker_id = new_uuid()

    async def _scenario() -> None:
        resolution = scope.prepare_worker_deps(
            binding, workflow="wf-claimed-without-deps", worker_id=worker_id
        )
        runner = FlowRunner(
            compiled,
            cast("Any", object()),
            "scope_probe_schema",
            worker_id=JobId(worker_id),
            deps_resolution=resolution,
        )
        with structlog.testing.capture_logs() as logs:
            tail = await runner._deps_tail(_deps_body, 1)  # pyright: ignore[reportPrivateUsage]  # Why: the runner-to-resolution seam supplies the resolving workflow's identity.
        assert isinstance(tail[0], _Probe)
        resolved = next(e for e in logs if e["event"] == "workflow-deps-resolved")
        assert resolved["workflow"] == "wf-scope-resolving-workflow"
        await scope.teardown_worker_deps(worker_id)

    asyncio.run(_scenario())


def test_an_injected_resolution_refuses_a_second_declared_binding() -> None:
    """One runner binds one deps declaration: an owned resolution cannot be
    combined with a ``deps=`` / ``deps_factory=`` the runner would own."""
    app = _scope_app(["wf-scope-refuse"])
    compiled = app.get("wf-scope-refuse")
    binding = compiled.deps_factory
    assert binding is not None
    resolution = DepsFactory(_make_probe)

    with pytest.raises(ValueError, match="deps_resolution="):
        FlowRunner(
            compiled,
            cast("Any", object()),
            "scope_probe_schema",
            deps=object(),
            deps_resolution=scope.WorkerDepsResolution(
                resolution, workflow="wf-scope-refuse", worker_id=new_uuid(), generation=0
            ),
        )


def test_two_event_loops_never_share_a_runner() -> None:
    """The runner cache is per event loop: a runner holds a loop's pool and
    deps resolution, so the same worker identity on a second loop gets a second
    runner, never the first loop's."""
    app = _scope_app(["wf-scope-loops"])
    app.get("wf-scope-loops")
    worker_id = JobId(new_uuid())
    pool = cast("Any", object())

    def _one_loop() -> object:
        async def _scenario() -> object:
            return seam._runner_for(  # pyright: ignore[reportPrivateUsage]  # Why: the door's runner factory is what the cache hands callers.
                "wf-scope-loops", pool, "scope_probe_schema", worker_id
            )

        return asyncio.run(_scenario())

    assert _one_loop() is not _one_loop(), "two loops must never share a runner"


def test_the_bootstrap_teardown_seam_evicts_the_workers_runner_cache() -> None:
    """The seam the exit stack calls releases the deps AND evicts the cached
    runner, so a later worker lifecycle in the same process cannot reuse a
    runner whose resolution was released with it."""
    app = _scope_app(["wf-scope-evict"])
    compiled = app.get("wf-scope-evict")
    binding = compiled.deps_factory
    assert binding is not None
    worker_id = new_uuid()

    async def _scenario() -> None:
        # a worker that never built a runner on this loop pays nothing
        await seam.teardown_worker_workflow_state(new_uuid())
        assert _ORDER == []
        resolution = scope.prepare_worker_deps(
            binding, workflow="wf-scope-evict", worker_id=worker_id
        )
        await resolution.get()
        pool = cast("Any", object())
        before = seam._runner_for(  # pyright: ignore[reportPrivateUsage]  # Why: the door's runner factory is the cache's only public face; the pin reads what it hands a caller.
            "wf-scope-evict",
            pool,
            "scope_probe_schema",
            JobId(worker_id),
            deps_resolution=resolution,
        )
        neighbour = new_uuid()
        neighbour_resolution = scope.prepare_worker_deps(
            binding, workflow="wf-scope-evict", worker_id=neighbour
        )
        neighbour_runner = seam._runner_for(  # pyright: ignore[reportPrivateUsage]  # Why: the neighbour's cached runner is the isolation assertion.
            "wf-scope-evict",
            pool,
            "scope_probe_schema",
            JobId(neighbour),
            deps_resolution=neighbour_resolution,
        )

        await seam.teardown_worker_workflow_state(worker_id)

        assert _ORDER == ["deps-teardown"]
        after = seam._runner_for(  # pyright: ignore[reportPrivateUsage]  # Why: same seam as above.
            "wf-scope-evict",
            pool,
            "scope_probe_schema",
            JobId(worker_id),
            deps_resolution=resolution,
        )
        assert after is not before, "the released runner was evicted from the cache"
        assert (
            seam._runner_for(  # pyright: ignore[reportPrivateUsage]  # Why: same cache face.
                "wf-scope-evict",
                pool,
                "scope_probe_schema",
                JobId(neighbour),
                deps_resolution=neighbour_resolution,
            )
            is neighbour_runner
        ), "one worker's teardown evicted its co-resident neighbour's live runner"

    asyncio.run(_scenario())


# ── the worker-hosted execution door (real PG) ───────────────────────────


@pytest.mark.integration
async def test_the_door_mints_one_instance_for_the_app_across_bodies_and_runs(
    wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    """N bodies x M runs across TWO workflows of one app: the factory runs
    once, on this worker's loop, and every body receives that exact instance."""
    app = _scope_app(["wf-scope-a", "wf-scope-b"])
    compiled_a = app.get("wf-scope-a")
    compiled_b = app.get("wf-scope-b")
    worker_id = new_uuid()

    flows = [
        await _create_run(wf_pool, wf_schema, compiled_a, "a1"),
        await _create_run(wf_pool, wf_schema, compiled_a, "a2"),
        await _create_run(wf_pool, wf_schema, compiled_b, "b1"),
    ]
    for flow_id in flows:
        execution = await _claim_and_execute(wf_pool, wf_schema, flow_id, worker_id)
        assert execution.outcome == "succeeded", execution

    assert _PRODUCED[0] == 1, f"the factory ran {_PRODUCED[0]}x; the scope leaked"
    assert len(_INSTANCES) == 3, _EXECUTED
    first = _INSTANCES[0]
    assert all(instance is first for instance in _INSTANCES), (
        "every body of the app received the same instance"
    )
    assert cast("_Probe", first).loop is asyncio.get_running_loop()


@pytest.mark.integration
async def test_two_workers_resolve_distinct_instances(
    wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    """Two workers of one fleet process resolve one instance each — the
    worker identity is part of the scope, never shared."""
    app = _scope_app(["wf-scope-two-workers"])
    compiled = app.get("wf-scope-two-workers")
    first_worker, second_worker = new_uuid(), new_uuid()

    first_flow = await _create_run(wf_pool, wf_schema, compiled, "w1")
    second_flow = await _create_run(wf_pool, wf_schema, compiled, "w2")
    await _claim_and_execute(wf_pool, wf_schema, first_flow, first_worker)
    await _claim_and_execute(wf_pool, wf_schema, second_flow, second_worker)

    assert _PRODUCED[0] == 2, f"two workers must mint two instances; got {_PRODUCED[0]}"
    assert len(_INSTANCES) == 2
    assert _INSTANCES[0] is not _INSTANCES[1]


@pytest.mark.integration
async def test_concurrent_first_claims_mint_exactly_one_instance(
    wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    """``max_concurrency > 1``'s shape at the door: two claimed rows execute
    body-first at the same instant, and the resolution's lock mints one
    instance for both."""
    app = _scope_app(["wf-scope-concurrent"], factory=_make_probe_slowly)
    compiled = app.get("wf-scope-concurrent")
    worker_id = new_uuid()

    flows = [
        await _create_run(wf_pool, wf_schema, compiled, "c1"),
        await _create_run(wf_pool, wf_schema, compiled, "c2"),
    ]
    executions = await asyncio.gather(
        _claim_and_execute(wf_pool, wf_schema, flows[0], worker_id),
        _claim_and_execute(wf_pool, wf_schema, flows[1], worker_id),
    )

    assert [e.outcome for e in executions] == ["succeeded", "succeeded"]
    assert _PRODUCED[0] == 1, f"concurrent first claims minted {_PRODUCED[0]} instances"
    assert len(_INSTANCES) == 2 and _INSTANCES[0] is _INSTANCES[1]


@pytest.mark.integration
async def test_the_worker_teardown_releases_the_door_resolution_once(
    wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    """The same teardown seam the worker's exit stack calls: after a body
    resolved through the door, it releases the instance exactly once (a second
    call releases nothing) and evicts the door's runner."""
    app = _scope_app(["wf-scope-door-teardown"])
    compiled = app.get("wf-scope-door-teardown")
    worker_id = new_uuid()
    flow_id = await _create_run(wf_pool, wf_schema, compiled, "t1")
    await _claim_and_execute(wf_pool, wf_schema, flow_id, worker_id)
    assert _PRODUCED[0] == 1

    await seam.teardown_worker_workflow_state(worker_id)
    await seam.teardown_worker_workflow_state(worker_id)

    assert _ORDER == ["deps-teardown"], f"exactly one release; got {_ORDER}"


# ── the real worker process (``_bootstrap._main``) ───────────────────────

_CRASH_NOW: dict[str, asyncio.Event] = {}


async def _crashing_heartbeat(*args: object, **kwargs: object) -> None:
    """A sibling that crashes on demand — the TaskGroup unwind's trigger (the
    same fault class a leader sweep hitting a dead PG produces)."""
    await _CRASH_NOW["event"].wait()
    raise RuntimeError("heartbeat hit a dead pg")


def _worker_settings(
    pg_dsn: str, schema: str, *, tag: str, max_concurrency: int = 1
) -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": pg_dsn,
            "TASKQ_SCHEMA_NAME": schema,
            "TASKQ_MAX_CONCURRENCY": str(max_concurrency),
            "TASKQ_HEALTH_ENABLED": "false",
            "TASKQ_HEALTH_SOCKET_PATH": unique_health_sock_path(f"scope_{tag}"),
            # the loop-lag watchdog's dumps would drown the capture; nothing
            # here blocks the loop, so the tiers stay out of the way.
            "TASKQ_LOOP_LAG_WARN_BUDGET": "9999",
            "TASKQ_LOOP_LAG_BUDGET": "9999",
        }
    )


def _tap_pool_closes(monkeypatch: pytest.MonkeyPatch) -> None:
    """Record each role pool's bounded close beside the deps release markers —
    the before-pool-close proof (``test_worker_deps_teardown``'s seam)."""
    real_close = worker_deps_mod.close_pool_bounded  # pyright: ignore[reportPrivateImportUsage]  # Why: the close site reads this module global at call time; capturing it is how the tap keeps calling the real bounded close.

    async def _tapped_close(
        pool: asyncpg.Pool, label: str, close_timeout: float, *, family: Any = "teardown"
    ) -> None:
        _ORDER.append(f"pool:{label}")
        await real_close(pool, label, close_timeout, family=family)

    monkeypatch.setattr(
        worker_deps_mod,
        "close_pool_bounded",  # pyright: ignore[reportPrivateImportUsage]  # Why: the call site reads this module global at close time, so replacing it here is the only way to observe the pool-close order.
        _tapped_close,
    )
    # The conditional slot pool's close callback lives in _bootstrap, whose
    # imported close helper is a separate module-global binding.
    monkeypatch.setattr(bootstrap_mod, "close_pool_bounded", _tapped_close)


def _assert_released_before_the_pools(*, slot_pool: bool = False) -> None:
    assert _ORDER.count("deps-teardown") == 1, f"exactly one release; got {_ORDER}"
    release_at = _ORDER.index("deps-teardown")
    labels = (
        ("dispatcher", "heartbeat", "worker", "slot")
        if slot_pool
        else ("dispatcher", "heartbeat", "worker")
    )
    for label in labels:
        marker = f"pool:{label}"
        assert marker in _ORDER, f"the {label} pool never closed; order={_ORDER}"
        assert release_at < _ORDER.index(marker), (
            f"the deps release must complete before the {label} pool closes; order={_ORDER}"
        )


def _assert_no_loop_or_pool_errors(logs: list[dict[str, Any]]) -> None:
    forbidden = ("Event loop is closed", "attached to a different loop", "pool is closed")
    offending = [entry for entry in logs if any(text in repr(entry) for text in forbidden)]
    assert offending == [], offending


@pytest.mark.integration
@pytest.mark.parametrize(
    ("failing_release", "with_slot_pool"),
    [(False, False), (True, False), (False, True)],
    ids=["clean", "raising", "slot-pool"],
)
async def test_a_graceful_shutdown_releases_the_deps_before_the_pools_close(
    module_pg_schema: ModulePgSchema,
    monkeypatch: pytest.MonkeyPatch,
    failing_release: bool,
    with_slot_pool: bool,
) -> None:
    """The real worker process end to end: it claims and executes workflow
    bodies through its own dispatch, and its graceful shutdown releases the
    factory-created deps exactly once — after the TaskGroup drained, before
    ``open_worker_deps`` closes a single role pool.

    The run's root row is stamped terminal here rather than waiting for the
    maintenance leader's sweep interval: ``status='succeeded'`` on the
    ``__flow__`` root is exactly what that seat writes for a completed run,
    and the drain monitor reads the same rows an operator does."""
    schema = module_pg_schema.schema_name
    app = _scope_app(
        ["wf-scope-graceful"], factory=_make_failing_probe if failing_release else _make_probe
    )
    compiled = app.get("wf-scope-graceful")
    pool = await asyncpg.create_pool(module_pg_schema.pg_dsn)
    loop_conn: asyncpg.Connection | None = None
    try:
        flow_id = await _create_run(pool, schema, compiled, "graceful")
        _tap_pool_closes(monkeypatch)
        settings = _worker_settings(
            module_pg_schema.pg_dsn,
            schema,
            tag="graceful",
            max_concurrency=2 if with_slot_pool else 1,
        )
        registry = ProviderRegistry() if with_slot_pool else None
        loop_conn = await asyncpg.connect(module_pg_schema.pg_dsn) if with_slot_pool else None
        if registry is not None and loop_conn is not None:
            registry.register_value(asyncpg.Connection, Scope.LOOP, loop_conn)

        with structlog.testing.capture_logs() as logs:
            worker = asyncio.create_task(
                bootstrap_mod._main(  # pyright: ignore[reportPrivateUsage]  # Why: the real worker entry point is the composition under test; its own bootstrap is the wiring being proven.
                    settings,
                    actor_registry={},
                    _registry=registry,
                    until_idle=True,
                    idle_settle_window=0.2,
                    idle_poll_interval=0.1,
                    idle_max_runtime=60.0,
                )
            )
            await wait_for_condition(
                lambda: len(_EXECUTED) == 1,
                description="the worker claimed and executed the workflow body",
                timeout=30.0,
            )
            await pool.execute(
                f"UPDATE \"{schema}\".jobs SET status = 'succeeded', "  # noqa: S608  # Why: the schema identifier is the fixture's own.
                "finished_at = clock_timestamp() WHERE id = $1 AND step_key = '__flow__'",
                flow_id,
            )
            exit_code = await asyncio.wait_for(worker, timeout=60.0)

        assert exit_code == 0, exit_code
        assert _PRODUCED[0] == 1, f"the factory ran {_PRODUCED[0]}x"
        _assert_released_before_the_pools(slot_pool=with_slot_pool)
        _assert_no_loop_or_pool_errors(logs)

        resolved = [e for e in logs if e["event"] == "workflow-deps-resolved"]
        torn = [e for e in logs if e["event"] == "workflow-deps-torn-down"]
        assert len(resolved) == 1, resolved
        assert resolved[0]["workflow"] == "wf-scope-graceful"
        # the boot bound the worker id into the log context; the events name it.
        assert resolved[0]["generation"] == scope.INITIAL_DEPS_GENERATION
        budget = next(
            e
            for e in logs
            if e["event"] == "workflow-deps-shutdown-budget-exceeds-termination-grace"
        )
        assert budget["workflow_deps_bindings"] == 1
        assert budget["worst_case_seconds"] == 82.0 + scope._TEARDOWN_TIMEOUT_SECS  # pyright: ignore[reportPrivateUsage]  # Why: the startup warning must model the same per-resource bound the scope applies.
        if failing_release:
            # a failing release is reported with its cause and must not stop
            # the worker's exit stack (the pools above still closed).
            failure = next(e for e in logs if e["event"] == "workflow-deps-teardown-error")
            assert failure["error_class"] == "RuntimeError"
            assert "deps release exploded" in failure["error"]
            assert failure["worker_id"] == resolved[0]["worker_id"]
            assert torn == [], "a failed release is not reported as a clean teardown"
        else:
            assert len(torn) == 1, torn
            assert torn[0]["worker_id"] == resolved[0]["worker_id"]
    finally:
        await pool.close()
        if loop_conn is not None:
            await loop_conn.close()


@pytest.mark.integration
async def test_a_sibling_crash_unwinds_through_the_release_before_the_pools_close(
    module_pg_schema: ModulePgSchema,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The crash-recovery exit path: a crashing sibling aborts the TaskGroup,
    the worker's exit stack still releases the deps exactly once (before the
    role pools close), and the crash surfaces — it is never swallowed."""
    schema = module_pg_schema.schema_name
    app = _scope_app(["wf-scope-crash"])
    compiled = app.get("wf-scope-crash")
    pool = await asyncpg.create_pool(module_pg_schema.pg_dsn)
    try:
        await _create_run(pool, schema, compiled, "crash")
        _CRASH_NOW["event"] = asyncio.Event()
        monkeypatch.setattr(bootstrap_mod, "heartbeat_loop", _crashing_heartbeat)
        _tap_pool_closes(monkeypatch)
        settings = _worker_settings(module_pg_schema.pg_dsn, schema, tag="crash")

        with structlog.testing.capture_logs() as logs:
            worker = asyncio.create_task(
                bootstrap_mod._main(settings, actor_registry={})  # pyright: ignore[reportPrivateUsage]  # Why: the real worker entry point; the crash path is the wiring under test.
            )
            await wait_for_condition(
                lambda: len(_EXECUTED) == 1,
                description="the worker claimed and executed the workflow body",
                timeout=30.0,
            )
            _CRASH_NOW["event"].set()
            crashed = False
            try:
                await asyncio.wait_for(worker, timeout=60.0)
            except BaseExceptionGroup:
                crashed = True

        assert crashed, "the sibling crash must propagate out of the worker"
        assert _PRODUCED[0] == 1
        _assert_released_before_the_pools()
        _assert_no_loop_or_pool_errors(logs)
        assert [e["event"] for e in logs if e["event"] == "workflow-deps-torn-down"]
    finally:
        await pool.close()

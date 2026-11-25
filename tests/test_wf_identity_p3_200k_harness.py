"""Regression tests for the 200k identity proof's harness contracts.

``tests/test_wf_identity_p3_200k.py`` pushes 200 pages (200,000 records at
1,000 records/page) through a two-pod fleet. Three harness invariants keep
that proof reliable and its failures readable:

1. Deadline ordering: the inner orchestration timeout must cover the
   projected worst-case 200-page CI runtime, sit strictly inside the outer
   ``pytest-timeout`` marker (whose clock starts before fleet bootstrap),
   and both must sit inside the ``slow-suite`` job's ``timeout-minutes``
   cap so a real overrun produces a deterministic ``TimeoutError`` rather
   than an asynchronous ``SIGALRM`` ``Failed`` or a silent job cancel.
2. Structured step-batch cancellation: claimed step batches in ``work()``
   must cancel and await every sibling ``run_fleet_claimed_step`` task on
   both inner timeout and single-step failure before ``_exercise`` tears
   down pools and runs ``DROP SCHEMA ... CASCADE``.
3. Page sharing: no pod may claim a whole 1,000-record page, so both
   worker pods work every dispatched page instead of one pod starving
   while the other spins on empty dispatch rounds.
"""

from __future__ import annotations

import ast
import asyncio
import contextlib
import inspect
from collections.abc import AsyncGenerator, Sequence
from pathlib import Path
from typing import Any, cast

import asyncpg
import pytest
import yaml

import tests.test_wf_identity_p3_200k as identity_200k
from taskq.backend._protocol import JobRow
from taskq.workflows import FlowRunner
from taskq.workflows import _worker_execution as wf_worker_execution
from taskq.workflows import definitions as wf_definitions
from tests._fleet import Pod
from tests._ns_patch import patch_ns

#: Worst-case per-page runtime projected for the ``slow-suite`` runner. The
#: former 1,800 s outer deadline truncated timed-out runs mid-proof, and every
#: page the source parks at saturation logs one ``wf.emit_backpressured`` line,
#: so those runs measured the rate directly: four sampled jobs parked 178 pages
#: in 1,796 s, 174 in 1,795 s, 172 in 1,788 s, and 170 in 1,797 s (10.15-10.63
#: s/page). Those are a lower bound on the degraded rate rather than an upper
#: bound on runner starvation, so this pin projects the issue's slowest
#: headline rate over 200 pages: 11.1 s/page -> 2,220 s.
_MEASURED_SLOWEST_PAGE_SECONDS = 11.1

#: One emitted page: the smallest batch that exercises multi-task step dispatch.
_ONE_PAGE = 1000

_SHIPPED_TEST = identity_200k.test_real_200k_unique_records_at_shipped_default_bound
_REPO_ROOT = Path(__file__).resolve().parents[1]
_CI_WORKFLOW = _REPO_ROOT / ".github" / "workflows" / "ci.yaml"
_SLOW_SUITE_JOB = "slow-suite"

#: Real step-execution door, preserved so non-injected steps (such as
#: ``source``) run through the production path.
_REAL_RUN_CLAIMED_STEP = FlowRunner.run_fleet_claimed_step

_INNER_TIMEOUT_CALLS = frozenset({"asyncio.wait_for", "wait_for", "asyncio.timeout", "timeout"})


def _outer_timeout_seconds() -> float:
    """Return the 200k proof's outer ``pytest-timeout`` budget in seconds."""
    marks: list[pytest.Mark] = list(getattr(_SHIPPED_TEST, "pytestmark", ()))
    for mark in marks:
        if mark.name == "timeout":
            return float(mark.args[0])
    raise AssertionError(
        "test_real_200k_unique_records_at_shipped_default_bound declares no "
        "pytest-timeout marker (falling back to pyproject's 300s default)"
    )


def _dotted_name(function: ast.expr) -> str:
    parts: list[str] = []
    node: ast.expr = function
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if isinstance(node, ast.Name):
        parts.append(node.id)
    return ".".join(reversed(parts))


def _resolve_numeric_expr(expression: ast.expr) -> float:
    """Resolve a timeout expression to seconds from literals, module constants,
    or ``_exercise`` parameter defaults."""
    if isinstance(expression, ast.Constant) and isinstance(expression.value, int | float):
        return float(expression.value)
    if isinstance(expression, ast.Name):
        module_val = getattr(identity_200k, expression.id, None)
        if isinstance(module_val, int | float):
            return float(module_val)
        param = inspect.signature(identity_200k._exercise).parameters.get(expression.id)
        if param is not None and isinstance(param.default, int | float):
            return float(param.default)
    if isinstance(expression, ast.BinOp):
        left = _resolve_numeric_expr(expression.left)
        right = _resolve_numeric_expr(expression.right)
        if isinstance(expression.op, ast.Add):
            return left + right
        if isinstance(expression.op, ast.Sub):
            return left - right
        if isinstance(expression.op, ast.Mult):
            return left * right
        if isinstance(expression.op, ast.Div):
            return left / right
    raise AssertionError(
        "cannot resolve inner timeout expression to seconds: "
        f"{ast.dump(expression)}; declare it as a literal, module constant, "
        "or _exercise default"
    )


def _inner_timeout_seconds() -> float:
    """Return the inner orchestration deadline in ``tests/test_wf_identity_p3_200k.py``."""
    tree = ast.parse(Path(identity_200k.__file__).read_text(encoding="utf-8"))
    found: list[float] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        name = _dotted_name(node.func)
        if name in {"asyncio.wait_for", "wait_for"}:
            for keyword in node.keywords:
                if keyword.arg == "timeout":
                    found.append(_resolve_numeric_expr(keyword.value))
            if not any(kw.arg == "timeout" for kw in node.keywords) and len(node.args) >= 2:
                found.append(_resolve_numeric_expr(node.args[1]))
        elif name in _INNER_TIMEOUT_CALLS and node.args:
            found.append(_resolve_numeric_expr(node.args[0]))
    assert found, (
        "tests/test_wf_identity_p3_200k.py declares no inner orchestration timeout; "
        "without one an overrun is killed by pytest-timeout's SIGALRM"
    )
    assert len(found) == 1, f"expected one inner orchestration timeout, found {found}"
    return found[0]


def _slow_suite_job_timeout_seconds() -> float:
    workflow = cast("dict[str, Any]", yaml.safe_load(_CI_WORKFLOW.read_text(encoding="utf-8")))
    job = cast("dict[str, Any]", workflow["jobs"][_SLOW_SUITE_JOB])
    cap_minutes = job.get("timeout-minutes")
    assert cap_minutes is not None, f"{_SLOW_SUITE_JOB} declares no timeout-minutes"
    return float(cap_minutes) * 60.0


def test_inner_timeout_covers_measured_200k_runtime() -> None:
    """The inner orchestration timeout must cover 200 pages at the issue's
    slowest reported CI page rate (11.1 s/page -> 2,220 s)."""
    pages = identity_200k._RECORDS // identity_200k._PAGE
    measured_seconds = pages * _MEASURED_SLOWEST_PAGE_SECONDS
    inner = _inner_timeout_seconds()
    assert inner >= measured_seconds, (
        f"inner orchestration timeout ({inner:.0f}s) is below the {measured_seconds:.0f}s "
        f"required for {pages} pages at {_MEASURED_SLOWEST_PAGE_SECONDS}s/page"
    )


def test_inner_timeout_is_strictly_less_than_outer_timeout() -> None:
    """Because the inner clock starts after fleet bootstrap while the outer
    ``pytest-timeout`` starts at test entry, equal budgets guarantee that the
    outer SIGALRM fires first."""
    inner = _inner_timeout_seconds()
    outer = _outer_timeout_seconds()
    assert inner < outer, (
        f"inner orchestration timeout ({inner:.0f}s) is not strictly less than "
        f"outer pytest-timeout ({outer:.0f}s); the outer SIGALRM will fire first"
    )


def test_outer_timeout_reserves_non_orchestration_margin() -> None:
    """Strict ordering alone is insufficient: bootstrap, verification, and
    teardown run outside the inner deadline but inside the outer one."""
    inner = _inner_timeout_seconds()
    outer = _outer_timeout_seconds()
    margin = outer - inner
    assert margin >= 300.0, (
        f"only {margin:.0f}s remains for fleet bootstrap, post-flight verification, "
        "and schema teardown; the outer SIGALRM can fire before the inner TimeoutError"
    )


def test_slow_suite_job_timeout_exceeds_outer_timeout() -> None:
    """The CI job cap must outlive the test's outer ``pytest-timeout`` so a
    timeout fails the test with a diagnostic instead of cancelling the lane."""
    outer = _outer_timeout_seconds()
    job_cap = _slow_suite_job_timeout_seconds()
    assert job_cap > outer, (
        f"{_SLOW_SUITE_JOB} job timeout ({job_cap:.0f}s) does not exceed the "
        f"test's outer pytest-timeout ({outer:.0f}s)"
    )


def test_slow_suite_job_timeout_reserves_lane_overhead() -> None:
    """The job also needs setup, co-tenant, and reporting time beyond the
    proof's own outer deadline; mere strict ordering does not reserve it."""
    outer = _outer_timeout_seconds()
    job_cap = _slow_suite_job_timeout_seconds()
    margin = job_cap - outer
    assert margin >= 600.0, (
        f"only {margin:.0f}s remains between the proof's outer timeout and the "
        f"{_SLOW_SUITE_JOB} job cap for lane setup, co-tenants, and reporting"
    )


class _SocketWriteError(Exception):
    """Stands in for an asyncpg socket-transport write failure mid-step."""


#: How long an interrupted step task takes to finish unwinding, standing in for
#: an asyncpg query's cancellation and rollback. Only a watchdog completes it,
#: starting when the first sibling is cancelled, so an abandoned batch is still
#: unwinding when the pools close and an awaited one holds teardown open until
#: it is done. It has to outlast every other thing ``_exercise`` does between the
#: cancellation and ``pool.close()``, so it is generous by an order of
#: magnitude: a run that leaks is caught well inside it.
_SIBLING_UNWIND_S = 1.0


def _live_step_tasks(baseline: frozenset[asyncio.Task[Any]]) -> list[asyncio.Task[Any]]:
    current = asyncio.current_task()
    return [
        task for task in asyncio.all_tasks() - baseline if task is not current and not task.done()
    ]


@pytest.fixture
def isolated_workflow_registry(monkeypatch: pytest.MonkeyPatch) -> None:
    """Give each harness test a fresh process-global workflow registry so
    calling ``_exercise`` multiple times never collides on the shared
    ``identity_scale_records`` registration."""
    monkeypatch.setattr(wf_definitions, "_default_registry", wf_definitions.WorkflowRegistry())
    monkeypatch.setattr(wf_worker_execution, "_compiled", {})


@pytest.mark.integration
async def test_inner_timeout_cancels_and_awaits_all_step_tasks_before_teardown(
    pg_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
    isolated_workflow_registry: None,
) -> None:
    """When the inner orchestration timeout fires while a page is in flight,
    every claimed step task must be cancelled and awaited before ``finally``
    closes pools and runs ``DROP SCHEMA ... CASCADE``.

    With a bare ``asyncio.gather``, the first child that finishes cancelling
    immediately resolves the gather with ``CancelledError`` while sibling tasks
    still unwinding (for example, cancelling an in-flight Postgres query) keep
    running into teardown.
    """
    sibling_cancelled = asyncio.Event()
    sibling_release = asyncio.Event()
    siblings_unwound = False

    async def release_siblings() -> None:
        nonlocal siblings_unwound
        await sibling_cancelled.wait()
        await asyncio.sleep(_SIBLING_UNWIND_S)
        siblings_unwound = True
        sibling_release.set()

    release_task = asyncio.create_task(release_siblings())
    baseline = frozenset(asyncio.all_tasks())
    record_calls = 0
    batch_ready = asyncio.Event()
    real_wait_for = asyncio.wait_for
    real_pool_close = asyncpg.Pool.close
    live_at_first_pool_close: list[asyncio.Task[Any]] | None = None

    async def injected_step(
        self: FlowRunner,
        flow_id: Any,
        row: asyncpg.Record,
        *,
        attempt: int,
        claim_epoch: int,
        run_priority: int | None = None,
    ) -> Any:
        nonlocal record_calls
        if row["step_key"] != "record":
            return await _REAL_RUN_CLAIMED_STEP(
                self,
                flow_id,
                row,
                attempt=attempt,
                claim_epoch=claim_epoch,
                run_priority=run_priority,
            )
        record_calls += 1
        call_index = record_calls
        if call_index >= 2:
            batch_ready.set()
        if call_index == 1 or call_index % 5:
            # Mirrors a coroutine queued on ``pool.acquire()``: it cancels on the
            # first tick, and one of these is enough to resolve a bare gather.
            await asyncio.Event().wait()
            return "succeeded"
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            # Mirrors an asyncpg query cancelling and rolling back: the task
            # finishes only once the watchdog releases it.
            sibling_cancelled.set()
            await sibling_release.wait()
            raise
        return "succeeded"

    real_timeout = asyncio.timeout

    async def shortened_wait_for(aw: Any, *args: Any, **kwargs: Any) -> Any:
        if getattr(aw, "__name__", None) == "orchestrate":
            await real_wait_for(batch_ready.wait(), timeout=30.0)
            return await real_wait_for(aw, timeout=0.05)
        declared = kwargs.get("timeout", args[0] if args else None)
        if isinstance(declared, int | float) and declared > 60:
            # Fail fast instead of sitting on the shipped 2,700 s deadline for
            # the whole outer pytest-timeout: a long-budget wait this pin did
            # not recognise is ``_exercise``'s own deadline under a new name.
            raise AssertionError(
                "the inner deadline moved to an unrecognised coroutine "
                f"({getattr(aw, '__name__', aw)!r}); update this pin's shortening"
            )
        return await real_wait_for(aw, *args, **kwargs)

    @contextlib.asynccontextmanager
    async def shortened_timeout(_delay: float | None) -> AsyncGenerator[None]:
        async with real_timeout(30.0) as cm:

            async def _arm_short_deadline() -> None:
                await batch_ready.wait()
                cm.reschedule(asyncio.get_running_loop().time() + 0.05)

            arm_task = asyncio.create_task(_arm_short_deadline())
            try:
                yield
            finally:
                arm_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await arm_task

    async def spy_pool_close(self: asyncpg.Pool) -> None:
        nonlocal live_at_first_pool_close
        if live_at_first_pool_close is None:
            live_at_first_pool_close = _live_step_tasks(baseline)
        return await real_pool_close(self)

    monkeypatch.setattr(FlowRunner, "run_fleet_claimed_step", injected_step)
    patch_ns(
        monkeypatch,
        identity_200k,
        "asyncio",
        wait_for=shortened_wait_for,
        timeout=shortened_timeout,
    )
    monkeypatch.setattr(asyncpg.Pool, "close", spy_pool_close)

    timed_out = False
    try:
        await identity_200k._exercise(pg_dsn, _ONE_PAGE)
    except* TimeoutError:
        timed_out = True

    remaining = _live_step_tasks(baseline)
    for task in remaining:
        task.cancel()
    for task in remaining:
        with contextlib.suppress(BaseException):
            await task
    release_task.cancel()
    with contextlib.suppress(BaseException):
        await release_task

    assert timed_out, "inner orchestration timeout did not raise TimeoutError"
    assert record_calls > 1, f"expected multiple claimed record steps, saw {record_calls}"
    assert siblings_unwound, (
        "_exercise tore the fleet down while the cancelled step batch was still "
        "unwinding after the inner timeout: the batch was abandoned, not awaited"
    )
    assert live_at_first_pool_close is not None, "teardown never reached pool.close()"
    assert not live_at_first_pool_close, (
        f"{len(live_at_first_pool_close)} of {record_calls} step tasks were still "
        "running when teardown began closing pools after inner timeout"
    )
    assert not remaining, f"{len(remaining)} step tasks remained pending after _exercise"


@pytest.mark.integration
async def test_claimed_step_failure_cancels_and_awaits_sibling_tasks(
    pg_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
    isolated_workflow_registry: None,
) -> None:
    """When one claimed step raises, every sibling step task in the batch must
    be cancelled and awaited before ``_exercise`` tears down the fleet.

    With a bare ``asyncio.gather``, the first exception immediately escapes
    ``work()`` without cancelling sibling tasks, leaving the rest of the claimed
    page live while ``finally`` closes pools and drops the schema.
    """
    baseline = frozenset(asyncio.all_tasks())
    record_calls = 0
    sibling_gate = asyncio.Event()

    async def injected_step(
        self: FlowRunner,
        flow_id: Any,
        row: asyncpg.Record,
        *,
        attempt: int,
        claim_epoch: int,
        run_priority: int | None = None,
    ) -> Any:
        nonlocal record_calls
        if row["step_key"] != "record":
            return await _REAL_RUN_CLAIMED_STEP(
                self,
                flow_id,
                row,
                attempt=attempt,
                claim_epoch=claim_epoch,
                run_priority=run_priority,
            )
        record_calls += 1
        if record_calls == 1:
            await asyncio.sleep(0)
            raise _SocketWriteError("Fatal write error on socket transport")
        await sibling_gate.wait()
        return "succeeded"

    monkeypatch.setattr(FlowRunner, "run_fleet_claimed_step", injected_step)

    try:
        with pytest.raises(BaseException) as failure:
            await identity_200k._exercise(pg_dsn, _ONE_PAGE)
    finally:
        sibling_gate.set()

    remaining = _live_step_tasks(baseline)
    for task in remaining:
        task.cancel()
    for task in remaining:
        with contextlib.suppress(BaseException):
            await task

    assert record_calls > 1, f"only {record_calls} record step ran (raised {failure.type.__name__})"
    assert not remaining, (
        f"{len(remaining)} of {record_calls} claimed step tasks were still running "
        f"after _exercise exited with {failure.type.__name__}"
    )


#: How long the page-sharing pin waits for the second pod to receive record
#: work. A pod that claimed the whole page cannot claim again while its own
#: batch runs, so a starved pod never receives any and this deadline is what
#: turns that stall into a failure instead of a hang.
_SECOND_POD_CLAIM_DEADLINE_S = 20.0


@pytest.mark.integration
async def test_both_pods_share_every_claimed_page(
    pg_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
    isolated_workflow_registry: None,
) -> None:
    """No pod may claim a whole page, and both worker pods must execute the
    records of the one page the proof dispatches.

    A whole-page claim hands all 1,000 records to one pod's twelve connections
    while the other pod burns ``dispatch_batch`` rounds that return nothing
    against the same Postgres — contention the proof pays for on every page
    without asserting anything.
    """
    baseline = frozenset(asyncio.all_tasks())
    claim_limits: list[int] = []
    records_per_pod: dict[str, int] = {}
    both_pods_claimed = asyncio.Event()
    real_claim = Pod.claim

    async def spying_claim(self: Pod, queues: Sequence[str], limit: int) -> list[JobRow]:
        claimed = await real_claim(self, queues, limit)
        claim_limits.append(limit)
        # ``chain_start`` riders carry the page's record under ``wf_item``, so
        # this counts record work alone — the flow root and the source step
        # claim through the same door.
        records = sum(1 for row in claimed if "wf_item" in row.payload)
        if records:
            records_per_pod[self.name] = records_per_pod.get(self.name, 0) + records
            if len(records_per_pod) > 1:
                both_pods_claimed.set()
        return claimed

    async def gated_step(
        self: FlowRunner,
        flow_id: Any,
        row: dict[str, Any],
        *,
        attempt: int,
        claim_epoch: int,
        run_priority: int | None = None,
    ) -> Any:
        # Hold every record batch until both pods hold record work. The
        # claiming pod is awaiting its own batch, so a whole-page claim keeps
        # the rest of the page out of the other pod's reach and this wait
        # expires with the page still owned by one pod.
        if row["step_key"] == "record":
            await asyncio.wait_for(both_pods_claimed.wait(), timeout=_SECOND_POD_CLAIM_DEADLINE_S)
        return await _REAL_RUN_CLAIMED_STEP(
            self,
            flow_id,
            row,
            attempt=attempt,
            claim_epoch=claim_epoch,
            run_priority=run_priority,
        )

    monkeypatch.setattr(Pod, "claim", spying_claim)
    monkeypatch.setattr(FlowRunner, "run_fleet_claimed_step", gated_step)

    failure: BaseException | None = None
    facts: dict[str, object] | None = None
    try:
        facts = await identity_200k._exercise(pg_dsn, _ONE_PAGE)
    except BaseException as exc:  # the assertions below carry the diagnosis
        failure = exc

    remaining = _live_step_tasks(baseline)
    for task in remaining:
        task.cancel()
    for task in remaining:
        with contextlib.suppress(BaseException):
            await task

    widest = max(claim_limits, default=0)
    assert widest <= identity_200k._PAGE // 2, (
        f"a pod claimed {widest} rows of a {identity_200k._PAGE}-record page; one pod then "
        "works the whole page while the other spins on empty dispatch rounds"
    )
    assert failure is None, f"the page's record work never reached both pods: {failure!r}"
    assert facts is not None
    worker_pods = cast("list[str]", facts["worker_pods"])
    assert set(records_per_pod) == set(worker_pods), (
        f"only {sorted(records_per_pod)} claimed records of the page, expected every pod in "
        f"{worker_pods} to take a share"
    )

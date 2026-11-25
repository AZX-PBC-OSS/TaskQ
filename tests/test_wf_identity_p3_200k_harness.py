"""Regression tests for the 200k identity proof's harness contracts.

``tests/test_wf_identity_p3_200k.py`` pushes 200 pages (200,000 records at
1,000 records/page) through a two-pod fleet. Three harness invariants keep
that proof reliable and its failures readable:

1. Deadline ordering: the inner orchestration timeout must cover the
   projected worst-case 200-page CI runtime, sit strictly inside the outer
   ``pytest-timeout`` marker (whose clock starts before fleet bootstrap),
   and both must sit inside the ``slow-suite`` job's ``timeout-minutes``
   cap so a real overrun exits through the inner deadline rather than an
   asynchronous ``SIGALRM`` ``Failed`` or a silent job cancel. The nesting
   re-raises that deadline wrapped: the outer ``TaskGroup`` surfaces an
   ``ExceptionGroup`` whose leaf is the ``TimeoutError`` — the shape
   ``except* TimeoutError`` matches, never a bare ``TimeoutError``.
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
import re
from collections.abc import AsyncGenerator, Sequence
from pathlib import Path
from typing import Any, cast

import asyncpg
import pytest
import yaml

import tests.test_wf_identity_p3_200k as identity_200k
from taskq.backend._protocol import JobRow
from taskq.workflows import FlowRunner
from taskq.workflows import _emit as emit_module
from taskq.workflows import _worker_execution as wf_worker_execution
from taskq.workflows import definitions as wf_definitions
from taskq.workflows.api._sql_runner import ITEM_KEY
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

#: Two emitted pages: the smallest run in which the shipped default bound
#: (``max_in_flight == _PAGE``) must park its source at least once.
_TWO_PAGES = identity_200k._PAGE * 2

_SHIPPED_TEST = identity_200k.test_real_200k_unique_records_at_shipped_default_bound
_REPO_ROOT = Path(__file__).resolve().parents[1]
_CI_WORKFLOW = _REPO_ROOT / ".github" / "workflows" / "ci.yaml"
_SLOW_SUITE_JOB = "slow-suite"

#: The lanes whose caps sit far below the proof's outer marker; the proof's
#: ``slow`` mark is the only thing keeping it out of them.
_FAST_SUITE_JOBS = ("test", "coverage-shard")

#: Every integration drive in this module opens a real two-pod fleet against
#: the one shared Postgres container. An explicit group keeps ``-n 2`` from
#: running two fleets at once: under ``--dist=loadgroup`` the conftest's
#: injected per-module group is invisible to the scheduler (``xdist`` stamps
#: the group suffix before that hook adds the marker), so the mark must be
#: declared here, the pattern the shipped proof uses.
_DRIVE_GROUP = "wf_identity_p3_harness"

#: The slow lane's longest co-tenant per-test budget (the lost-job soak's and
#: the compose stack e2e's 900 s ``pytest-timeout`` markers).
_LANE_CO_TENANT_BUDGET_S = 900.0

#: What the job cap must reserve beyond the proof's own outer deadline: the
#: 900 s co-tenant budget plus lane setup and reporting.
_LANE_OVERHEAD_RESERVE_S = _LANE_CO_TENANT_BUDGET_S + 300.0

#: The ``-m`` marker expression of a lane's pytest suite run.
_MARKER_FILTER = re.compile(r'-m\s+"([^"]+)"')

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


def _ci_job(job_name: str) -> dict[str, Any]:
    workflow = cast("dict[str, Any]", yaml.safe_load(_CI_WORKFLOW.read_text(encoding="utf-8")))
    return cast("dict[str, Any]", workflow["jobs"][job_name])


def _slow_suite_job_timeout_seconds() -> float:
    cap_minutes = _ci_job(_SLOW_SUITE_JOB).get("timeout-minutes")
    assert cap_minutes is not None, f"{_SLOW_SUITE_JOB} declares no timeout-minutes"
    return float(cap_minutes) * 60.0


def _job_marker_filters(job_name: str) -> list[str]:
    """Return the ``-m`` expressions of a job's suite runs.

    A job's side lanes (probes, benchmarks) carry their own explicit paths and
    filters; only the suite invocations — the sharded ``"${files[@]}"`` steps
    where present — select the tier these pins compare caps for.
    """
    runs: list[str] = []
    for step in cast("list[dict[str, Any]]", _ci_job(job_name).get("steps") or []):
        run = step.get("run")
        if isinstance(run, str) and "pytest" in run:
            runs.append(run)
    suite_runs = [run for run in runs if '"${files[@]}"' in run] or runs
    filters = [match for run in suite_runs for match in _MARKER_FILTER.findall(run)]
    assert filters, f"{job_name} runs no pytest step with a -m marker filter"
    return filters


def _marker_filter_matches(expression: str, marks: set[str]) -> bool:
    """Evaluate a lane's ``-m`` filter against a module's mark names.

    Only the conjunction of names and ``not <name>`` terms the CI filters use
    is supported; anything else fails closed so a richer filter cannot pass
    this pin by accident.
    """
    assert " or " not in expression, f"unsupported marker filter: {expression!r}"
    for term in expression.split(" and "):
        term = term.strip()
        negated = term.startswith("not ")
        name = term.removeprefix("not ").strip()
        assert name and " " not in name, f"unsupported marker filter term: {term!r}"
        if (name in marks) == negated:
            return False
    return True


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
    proof's own outer deadline; mere strict ordering does not reserve it.

    The reserve must model the lane: a co-tenant dispatched while the proof is
    finishing may legally run its own 900 s budget (the lost-job soak's and the
    compose stack e2e's per-test markers)."""
    outer = _outer_timeout_seconds()
    job_cap = _slow_suite_job_timeout_seconds()
    margin = job_cap - outer
    assert margin >= _LANE_OVERHEAD_RESERVE_S, (
        f"only {margin:.0f}s remains between the proof's outer timeout and the "
        f"{_SLOW_SUITE_JOB} job cap; the lane needs {_LANE_CO_TENANT_BUDGET_S:.0f}s for a "
        "co-tenant that starts while the proof finishes plus setup and reporting"
    )


def test_slow_suite_comment_headroom_matches_the_arithmetic() -> None:
    """The ``slow-suite`` comment is the only written rationale for the cap:
    the headroom it states must equal the numbers it quotes, and it must not
    claim a runner the job does not use."""
    workflow_text = _CI_WORKFLOW.read_text(encoding="utf-8")
    section = workflow_text.split(f"  {_SLOW_SUITE_JOB}:", 1)[1].split("  load-sensitive:", 1)[0]
    match = re.search(r"leaves\s+([\d,]+)\s*s\s*=\s*(\d+)\s*minutes", section)
    assert match is not None, (
        "the slow-suite comment no longer states its headroom as "
        "'leaves <seconds> s = <minutes> minutes'"
    )
    quoted_seconds = float(match.group(1).replace(",", ""))
    quoted_minutes = float(match.group(2))

    inner = _inner_timeout_seconds()
    outer = _outer_timeout_seconds()
    job_cap = _slow_suite_job_timeout_seconds()
    margin = job_cap - outer
    assert inner < outer < job_cap
    assert quoted_seconds == margin, (
        f"the slow-suite comment claims {quoted_seconds:.0f}s of headroom; "
        f"{job_cap:.0f}s - {outer:.0f}s = {margin:.0f}s"
    )
    assert quoted_minutes == margin / 60.0, (
        f"the slow-suite comment claims {quoted_minutes:.0f} minutes of headroom; "
        f"{job_cap:.0f}s - {outer:.0f}s = {margin / 60.0:.0f} minutes"
    )
    assert "8-core" not in section, (
        "the slow-suite comment claims a larger runner, but the job declares "
        "`runs-on: ubuntu-latest`"
    )


def test_lane_filters_select_the_proof_only_in_slow_suite() -> None:
    """The cap pins compare two numbers, so bind them to the relation that
    makes the comparison mean anything: the proof's module-level marks must
    satisfy ``slow-suite``'s ``-m`` filter and be excluded by the fast lanes'
    filters, whose caps sit below the proof's outer marker."""
    proof_marks = {
        mark.name
        for source in (identity_200k, _SHIPPED_TEST)
        for mark in getattr(source, "pytestmark", ())
    }
    assert {"integration", "slow"} <= proof_marks, (
        f"the 200k proof's module-level marks are {sorted(proof_marks)}; these pins assume "
        "it rides slow-suite as both integration and slow"
    )
    slow_filters = _job_marker_filters(_SLOW_SUITE_JOB)
    assert any(_marker_filter_matches(expression, proof_marks) for expression in slow_filters), (
        f"{_SLOW_SUITE_JOB}'s filters {slow_filters} do not select the 200k proof; the job "
        "cap these pins read is no longer the cap that governs the proof"
    )
    for job_name in _FAST_SUITE_JOBS:
        for expression in _job_marker_filters(job_name):
            assert not _marker_filter_matches(expression, proof_marks), (
                f"{job_name}'s filter {expression!r} selects the 200k proof; that job's cap "
                "fires before the proof's 50-minute outer marker can, the silent cancel this "
                "harness exists to prevent"
            )


def test_module_docstring_names_the_overrun_failure_shape() -> None:
    """A bare ``except TimeoutError`` written against this module's docstring
    must not silently catch nothing: the shipped nesting wraps the deadline."""
    documentation = __doc__ or ""
    assert "ExceptionGroup" in documentation, (
        "the harness docstring names the overrun failure as a deterministic TimeoutError, "
        "but the shipped nesting raises an ExceptionGroup wrapping one: a caller writing "
        "`except TimeoutError` against that sentence silently catches nothing"
    )


def _is_mark(node: ast.expr, attribute: str) -> bool:
    if isinstance(node, ast.Call):
        node = node.func
    return (
        isinstance(node, ast.Attribute)
        and node.attr == attribute
        and isinstance(node.value, ast.Attribute)
        and node.value.attr == "mark"
        and isinstance(node.value.value, ast.Name)
        and node.value.value.id == "pytest"
    )


def _module_level_group(tree: ast.Module) -> bool:
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        if not any(
            isinstance(target, ast.Name) and target.id == "pytestmark" for target in node.targets
        ):
            continue
        elements = (
            list(node.value.elts) if isinstance(node.value, ast.List | ast.Tuple) else [node.value]
        )
        if any(_is_mark(element, "xdist_group") for element in elements):
            return True
    return False


def _explicit_group_name(node: ast.expr) -> str | None:
    if not _is_mark(node, "xdist_group") or not isinstance(node, ast.Call) or not node.args:
        return None
    argument = node.args[0]
    if isinstance(argument, ast.Constant) and isinstance(argument.value, str):
        return argument.value
    return None


def test_every_integration_drive_declares_an_explicit_xdist_group() -> None:
    """Every ``integration`` test in this module opens a real fleet, and the
    conftest's injected group never reaches the ``loadgroup`` scheduler."""
    tree = ast.parse(Path(__file__).read_text(encoding="utf-8"))
    module_grouped = _module_level_group(tree)

    drives: list[str] = []
    missing: list[str] = []
    misgrouped: list[tuple[str, list[str]]] = []
    for node in tree.body:
        if not isinstance(node, ast.AsyncFunctionDef | ast.FunctionDef):
            continue
        decorators = list(node.decorator_list)
        if not any(_is_mark(decorator, "integration") for decorator in decorators):
            continue
        drives.append(node.name)
        if module_grouped:
            continue
        groups = {
            name
            for decorator in decorators
            if (name := _explicit_group_name(decorator)) is not None
        }
        if not groups:
            missing.append(node.name)
        elif groups != {_DRIVE_GROUP}:
            misgrouped.append((node.name, sorted(groups)))

    assert drives, "the harness no longer declares any integration drive"
    assert not missing, (
        "these fleet-opening drives carry no explicit xdist_group, so "
        "`--dist=loadgroup` gives each its own work unit and `-n 2` runs two "
        f"real 2-pod fleets concurrently on one runner: {missing}. "
        f"Declare `@pytest.mark.xdist_group({_DRIVE_GROUP!r})` on each, the pattern the "
        "shipped proof uses with `wf_identity_p3_scale`."
    )
    assert not misgrouped, (
        f"these integration drives declare a group other than {_DRIVE_GROUP!r}, so `-n 2` "
        f"may run them concurrently: {misgrouped}"
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
@pytest.mark.xdist_group("wf_identity_p3_harness")
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

    Every fifth record step of the cancelled batch runs through the real
    production door, not a synthetic coroutine: what the batch drains must
    include ``run_fleet_claimed_step``'s own claim-ledger unwind, since that is
    what a regression there would leave holding a connection through
    ``DROP SCHEMA``.
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
    real_record_calls = 0
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
        nonlocal record_calls, real_record_calls
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
        if call_index % 5 == 2:
            # Every fifth record step of the batch is the real production door,
            # so the drain this pin asserts also covers real claim-ledger
            # unwinds rather than only coroutines that hold no connection: with
            # 500 claimed records most of these are still in flight when the
            # inner deadline cancels the batch.
            real_record_calls += 1
            return await _REAL_RUN_CLAIMED_STEP(
                self,
                flow_id,
                row,
                attempt=attempt,
                claim_epoch=claim_epoch,
                run_priority=run_priority,
            )
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
            # Close the coroutine we never awaited so the drift path raises its
            # AssertionError without also emitting ``RuntimeWarning``.
            close = getattr(aw, "close", None)
            if callable(close):
                close()
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

    remaining: list[asyncio.Task[Any]] = []
    timed_out = False
    try:
        await identity_200k._exercise(pg_dsn, _ONE_PAGE)
    except* TimeoutError:
        timed_out = True
    finally:
        # Instrumentation cleanup runs on EVERY exit path, not only the inner
        # deadline: the drive's own fail-closed drift guard (or any other
        # ``_exercise`` failure) must not leave the watchdog pending for the
        # suite's leak gate to report on top of the real error.
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
    assert real_record_calls >= 1, (
        "the timeout drive cancelled only synthetic step doubles: route at least one record "
        "step through _REAL_RUN_CLAIMED_STEP so the production door's cancellation unwind is "
        "what this batch pin drains"
    )
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


def _pending_named(name: str) -> list[asyncio.Task[Any]]:
    current = asyncio.current_task()
    return [
        task
        for task in asyncio.all_tasks()
        if task is not current
        and not task.done()
        and getattr(task.get_coro(), "__name__", "") == name
    ]


async def test_failed_timeout_drive_leaves_no_watchdog_task_behind(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A non-``TimeoutError`` exit from ``_exercise`` must still cancel and
    await the drive's own ``release_siblings`` watchdog.

    The reachable trigger is the drive's fail-closed drift guard: the day the
    shipped deadline is re-spelled, that AssertionError is precisely what a
    maintainer needs to read, and it must not arrive buried under the leak
    gate's report of the drive's own instrumentation.
    """

    async def drifted_exercise(_dsn: str, _count: int) -> dict[str, object]:
        # The shape of a real drift: the orchestration deadline moved to a
        # coroutine this pin does not recognise. The guard raises before
        # awaiting the coroutine, so it propagates as an AssertionError.
        async def recosted_driver() -> None:
            await asyncio.sleep(0.0)

        await identity_200k.asyncio.wait_for(
            recosted_driver(), timeout=identity_200k._INNER_TIMEOUT_S
        )
        return {}

    monkeypatch.setattr(identity_200k, "_exercise", drifted_exercise)

    with pytest.raises(AssertionError, match="unrecognised coroutine"):
        await test_inner_timeout_cancels_and_awaits_all_step_tasks_before_teardown(
            "unused-dsn", monkeypatch, None
        )

    leaked = _pending_named("release_siblings")
    for task in leaked:
        task.cancel()
        with contextlib.suppress(BaseException):
            await task

    assert not leaked, (
        "the timeout drive exited without cancelling its own watchdog, so the central leak "
        "gate reports it on top of the real error"
    )


@pytest.mark.integration
@pytest.mark.xdist_group("wf_identity_p3_harness")
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
@pytest.mark.xdist_group("wf_identity_p3_harness")
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
    record_steps_entered = 0
    both_pods_claimed = asyncio.Event()
    real_claim = Pod.claim

    async def spying_claim(self: Pod, queues: Sequence[str], limit: int) -> list[JobRow]:
        claimed = await real_claim(self, queues, limit)
        claim_limits.append(limit)
        # ``chain_start`` riders carry the page's record under the canonical
        # ``ITEM_KEY``, so this counts record work alone — the flow root and
        # the source step claim through the same door.
        records = sum(1 for row in claimed if ITEM_KEY in row.payload)
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
        nonlocal record_steps_entered
        # Hold every record batch until both pods hold record work. The
        # claiming pod is awaiting its own batch, so a whole-page claim keeps
        # the rest of the page out of the other pod's reach and this wait
        # expires with the page still owned by one pod.
        if row["step_key"] == "record":
            record_steps_entered += 1
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
    spy_counted = sum(records_per_pod.values())
    assert spy_counted >= record_steps_entered, (
        f"the page-sharing spy counted {spy_counted} record rows across "
        f"{sorted(records_per_pod)} while gated_step entered {record_steps_entered} "
        f"record steps: it is blind to the record payload key {ITEM_KEY!r}, so the "
        f"diagnosis below would blame the proof's claim granularity for the spy's "
        f"own blindness (failure={failure!r})"
    )
    assert widest <= identity_200k._PAGE // 2, (
        f"a pod claimed {widest} rows of a {identity_200k._PAGE}-record page; one pod then "
        "works the whole page while the other spins on empty dispatch rounds"
    )
    assert failure is None, f"the page's record work never reached both pods: {failure!r}"
    assert spy_counted == _ONE_PAGE, (
        f"the page-sharing spy counted {spy_counted} record rows across "
        f"{sorted(records_per_pod)} while the proof dispatched {_ONE_PAGE}"
    )
    assert facts is not None
    worker_pods = cast("list[str]", facts["worker_pods"])
    assert set(records_per_pod) == set(worker_pods), (
        f"only {sorted(records_per_pod)} claimed records of the page, expected every pod in "
        f"{worker_pods} to take a share"
    )


def _exception_leaves(exc: BaseException) -> list[BaseException]:
    if isinstance(exc, BaseExceptionGroup):
        return [leaf for sub in exc.exceptions for leaf in _exception_leaves(sub)]
    return [exc]


@pytest.mark.integration
@pytest.mark.xdist_group("wf_identity_p3_harness")
async def test_a_proof_that_never_parks_its_source_must_not_report_success(
    pg_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
    isolated_workflow_registry: None,
) -> None:
    """A run whose source streams every page in one attempt exercised none of
    #717's backpressure, so the proof must refuse it itself.

    The fault injected here is capacity reported as always empty, so the
    emit's admission never raises ``EmitBackpressure`` and the source never
    parks. Every record, ledger, edge and cursor assertion still holds, and the
    source-ledger history check is satisfied vacuously by a single row — only
    the runtime park floor added to ``_exercise`` can catch this shape.
    """
    real_observe = emit_module.observe_emit_capacity

    async def never_full(*args: Any, **kwargs: Any) -> Any:
        observation = await real_observe(*args, **kwargs)
        return observation.__class__(
            live_rows=0,
            reservations=0,
            min_waiting_width=observation.min_waiting_width,
        )

    monkeypatch.setattr(emit_module, "observe_emit_capacity", never_full)

    facts: dict[str, object] | None = None
    failure: BaseException | None = None
    try:
        facts = await identity_200k._exercise(pg_dsn, _TWO_PAGES)
    except BaseException as exc:  # the assertions below carry the diagnosis
        failure = exc

    assert facts is None, (
        "the proof reported success for a run in which the shipped default bound "
        f"never refused a page ({_TWO_PAGES // identity_200k._PAGE} pages emitted in one "
        f"source attempt: source_calls={(facts or {}).get('source_calls')}), so the run "
        "exercised none of #717's backpressure and nothing in its contract noticed"
    )
    assert failure is not None
    assert any(isinstance(leaf, AssertionError) for leaf in _exception_leaves(failure)), (
        f"the run failed for a reason unrelated to the unexercised bound: {failure!r}"
    )

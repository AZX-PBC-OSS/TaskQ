"""The lifecycle plugin: the scope tree's pytest integration.

The invocation scope spans the session; a module scope spans one test
module; a fixture scope spans one fixture's setup and finalizer
(pytest-asyncio runs them in tasks sharing the fixture's captured
context, so the fixture scope is one lifetime); the test scope is
pushed around the call phase, so pytest-asyncio's copy_context at
runtest stamps the test task with it. The task factory is installed on
the runner loop at the guard's setup, before any test code runs, so
every task or Future a test creates is registered in the active scope
of its creating context; an unowned creation lands in the quarantine
and fails closed at the guard.

A hook wrapper is a sync generator and cannot await, so the hooks here
only push and pop the scope contextvar and record the item's scopes;
the closes (freeze, stop, natural grace, drain, abort, report) run in
the guard fixture's teardown, which is async and runs before the loop
fixture closes. The primary exception of a test is recorded
independently by pytest; the guard's report is the cleanup verdict, so
the precedence stays body > loop > drain. The static guard remains a
tripwire over the converted drivers; this registry is the isolation
guarantee.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Awaitable, Callable
from contextvars import Token

import pytest

from tests._lifecycle import (
    QUARANTINE,
    Disposition,
    LifecycleScope,
    ScopeKind,
    _active_scope,
)

_INVOCATION_SCOPE: LifecycleScope | None = None
_MODULE_SCOPES: dict[str, LifecycleScope] = {}
_FIXTURE_STACK: list[tuple[Token["LifecycleScope | None"], LifecycleScope]] = []

_ITEM_SCOPES_KEY: pytest.StashKey[list[LifecycleScope]] = pytest.StashKey()


def pytest_sessionstart(session: pytest.Session) -> None:
    global _INVOCATION_SCOPE
    _INVOCATION_SCOPE = LifecycleScope(  # pyright: ignore[reportConstantRedefinition]  # Why: the invocation scope is created once per session and reset at session finish.
        kind=ScopeKind.INVOCATION, name=session.name or "invocation"
    )


def _module_scope_for(item: pytest.Item) -> LifecycleScope:
    assert _INVOCATION_SCOPE is not None
    module_name = str(item.path)
    scope = _MODULE_SCOPES.get(module_name)
    if scope is None:
        scope = _INVOCATION_SCOPE.child(ScopeKind.MODULE, module_name)
        _MODULE_SCOPES[module_name] = scope
    return scope


@pytest.hookimpl(wrapper=True)
def pytest_fixture_setup(
    fixturedef: pytest.FixtureDef[object], request: pytest.FixtureRequest
) -> object:
    """A fixture scope around one fixture's setup and finalizer. The
    close is deferred to the guard's teardown (this hook cannot await);
    the scope is recorded on the item it belongs to."""
    if _INVOCATION_SCOPE is None:
        result = yield
        return result
    item = getattr(request, "_pyfuncitem", None)
    parent = _active_scope.get() or (
        _module_scope_for(item) if item is not None else _INVOCATION_SCOPE
    )
    scope = parent.child(ScopeKind.FIXTURE, fixturedef.argname)
    token = _active_scope.set(scope)
    _FIXTURE_STACK.append((token, scope))
    try:
        result = yield
        return result
    finally:
        _active_scope.reset(token)
        # Only a function-scoped fixture's scope closes with the item.
        # A wider fixture's scope outlives the test that triggered its
        # setup; its leases are inheritance, audited at the invocation
        # close, not this test's failures.
        if item is not None and fixturedef.scope == "function":
            item.stash.setdefault(_ITEM_SCOPES_KEY, []).append(scope)


@pytest.hookimpl(wrapper=True)
def pytest_runtest_call(item: pytest.Item) -> object:
    """The test scope around the call phase: stamped before pytest-asyncio
    creates the test task, so the task's context carries it. The close
    runs in the guard fixture's teardown."""
    module_scope = _module_scope_for(item)
    scope = module_scope.child(ScopeKind.TEST, item.name)
    token = _active_scope.set(scope)
    try:
        result = yield
        return result
    finally:
        _active_scope.reset(token)
        item.stash.setdefault(_ITEM_SCOPES_KEY, []).append(scope)


def quarantine_count() -> int:
    """The number of quarantine leases so far; the guard snapshots it at
    setup and reports the delta at teardown."""
    return len(QUARANTINE.leases)


async def close_item_scopes(
    item: pytest.Item,
    *,
    drain: Callable[[set[asyncio.Future[object]]], Awaitable[object]] | None = None,
    quarantine_since: int = 0,
    quarantine_until: float | None = None,
) -> str | None:
    """Close the item's fixture and test scopes with the teardown
    protocol and fail closed on new quarantine leases. Returns the
    failure report, or None when every lease ended clean (a benign
    release tail aside). The guard raises the report, so the test's
    primary exception keeps its precedence."""
    scopes = item.stash.get(_ITEM_SCOPES_KEY, [])
    item.stash[_ITEM_SCOPES_KEY] = []
    lines: list[str] = []
    for scope in scopes:
        await scope.close(drain=drain)
        for lease in scope.leases:
            # A managed-loop lease is exempt from the guard's report: the
            # supervisor's own contract raises the loop error and the
            # survivor RuntimeError into the body, so the primary
            # exception already carries the failure; the dispositions
            # stay recorded for the audit.
            if lease.kind == "managed-loop":
                continue
            if lease.disposition is Disposition.SURVIVOR:
                task = lease.task
                coro = repr(task.get_coro()) if task is not None else repr(lease.future)
                code = getattr(task.get_coro(), "cr_code", None) if task is not None else None
                lines.append(
                    f"  - {lease.kind} {lease.name!r} ({lease.phase.value}, "
                    f"{scope.kind.value} scope {scope.name!r}) survived the "
                    f"bound: {coro} from {getattr(code, '__module__', None)}."
                    f"{getattr(code, '__qualname__', None)}"
                )
            elif lease.disposition is Disposition.ABORTED and not lease.abrupt_subject:
                lines.append(
                    f"  - {lease.kind} {lease.name!r} ({lease.phase.value}, "
                    f"{scope.kind.value} scope) had to be aborted by the teardown"
                )
            elif lease.disposition is Disposition.FAILED:
                coro = repr(lease.task.get_coro()) if lease.task is not None else repr(lease.future)
                lines.append(
                    f"  - {lease.kind} {lease.name!r} ({lease.phase.value}, "
                    f"{scope.kind.value} scope) raised an exception the test "
                    f"never saw: {coro}"
                )
    for lease in QUARANTINE.leases[quarantine_since:]:
        if lease.task is None:
            continue
        # Unowned work minted inside the call window fails closed;
        # unowned work created after the call is teardown machinery and
        # is only retrieved, never failed here.
        in_window = quarantine_until is None or lease.created <= quarantine_until
        if lease.task.done():
            if not lease.task.cancelled():
                with contextlib.suppress(Exception):
                    lease.task.exception()
            lease.retrieved = True
        elif in_window:
            lines.append(
                f"  - unowned {lease.kind} {lease.name!r} "
                f"({lease.phase.value}) created outside every lifecycle scope"
            )
    if not lines:
        return None
    return (
        "the lifecycle registry closed this test's scopes with live or failed work:\n"
        + "\n".join(lines)
    )


def pytest_sessionfinish(session: pytest.Session, exitstatus: object) -> None:
    """The invocation scope's audit: leases still registered at session
    end, printed as the session's own report."""
    global _INVOCATION_SCOPE
    if _INVOCATION_SCOPE is None:
        return
    leftovers = [
        lease.describe() for lease in _INVOCATION_SCOPE.tree_leases() if lease.disposition is None
    ]
    if leftovers:
        print("\n=== lifecycle: work still registered at session end ===\n" + "\n".join(leftovers))
    _INVOCATION_SCOPE = None  # pyright: ignore[reportConstantRedefinition]  # Why: the invocation scope ends with the session.
    _MODULE_SCOPES.clear()

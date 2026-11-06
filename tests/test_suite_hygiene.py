"""Static hygiene guards for the test suite itself.

These are plain (non-PG, non-container) regression tests that grep the
``tests/`` tree and the published ``src/taskq/testing/`` package for
anti-patterns which cause cross-test and cross-worker
schema collisions under ``pytest-xdist``:

- ``os.environ.get("PYTEST_XDIST_WORKER", ...)``-derived schema names give
  no real isolation - every test file within one xdist worker resolves to
  the *same* string, so files sharing a worker mutually clobber each
  other's schema state. For the PUBLISHED testing package this is worse:
  a fixed per-worker default reaches consumer suites on shared-database
  models, where it clobbers across modules.
- Module-level ``_SCHEMA = ...`` / ``SCHEMA = ...`` constants encode the
  same anti-pattern (or go stale) and should instead be sourced
  from the ``module_pg_schema`` / ``clean_pg_conn`` / ``clean_jobs_app``
  fixtures, or a per-test unique name (e.g. ``f"prefix_{new_base62()}"``).

New test files must not reintroduce either pattern. This file is excluded
from its own scan (it necessarily mentions the patterns in prose/regex
form), and it also hosts the unit tests for the run-isolation naming seam
itself (``run_isolation_token`` / ``_module_db_name`` / the schema-name
helpers) - those tests necessarily set and assert ``PYTEST_XDIST_WORKER``,
which is the second reason the self-exemption exists. ``conftest.py``
files are excluded from the ``tests/`` scan: the conftest db-name helper
(``_module_db_name``) and the e2e schema helper use the worker id only as
one input to a per-module hash - the sanctioned worker-qualified-hash
pattern. ``src/taskq/testing/fixtures.py`` is allowlisted from the
worker-id scan for the same reason (``_schema_name_from_module`` /
``_schema_name_from_test`` hash the worker id together with the module
path / node id, so the name is never worker-only); every other file in
the published package is fully scanned.

The second half of the file guards a different hazard with the same shape:
``taskq[oidc]`` ships only ``httpx2`` (nothing under ``src/taskq`` imports
``httpx``), but the dev GROUP keeps ``httpx`` installed too, so the test
environment runs two HTTP client stacks that are invisible to each other's
mocks. A mock that covers only some of the stacks in use reads as a mock
while part of the traffic leaves the machine, or - worse - the suite
compensates by substituting one stack for the other and then tests a client
production never constructs. See the section comment there.
"""

# ── Static guards elsewhere in the suite: triage of record ────────────────
#
# A sweep of every test that read source text rather than exercising
# behaviour finished on this branch. Recorded here so the survivors are not
# mistaken for ones nobody looked at.
#
# deliberately static - codebase invariants with no runtime expression. A
# running system cannot be asked "does this module import that one at import
# time"; the property is structural, so the check is too. All are AST-based or
# an inventory of an artifact, not substring greps:
#   - tests/_import_discipline.py (+ its callers in web_admin/test_factory,
#     web_admin/test_sse, test_web_health) - module-level import coupling and
#     `from __future__ import annotations`.
#   - test_notify.py - ast.NodeVisitor guard on pool attribute access.
#   - test_retry_classifier.py - taskq.retry's import boundary from
#     taskq.backend. (Its AST walk runs in a subprocess for no reason that
#     survives inspection; harmless, and cosmetic to unpick.)
#   - test_leader_sweeps_coverage.py - the acquire-timeout AST invariant.
#   - test_scheduled_writers_audit.py - inventory of `mark_*` methods.
#   - test_sse_connection_caps.py::test_no_uncapped_sse_endpoint_remains -
#     inventory over the web package; it guards endpoints not yet written.
#   - test_max_concurrent_docs_contract.py, and the readme/deployment-guide
#     checks in test_migrate_on_start_worker.py - documentation contracts,
#     reading docs rather than source. Same shape as test_ci_workflow.py.
#   - this file - greps the test tree for anti-patterns, which is its job.
#
# The rule that settled each case: if a scanner's protection was already
# provided by behavioural tests, it was deleted rather than rewritten, and the
# claim was measured - revert the guarded thing, count what fails - rather than
# asserted. Files where that measurement was made carry the numbers in a
# comment at the site (test_schema_name_validator.py, test_shutdown_integration.py,
# test_obs_exception_redaction.py, test_settings_validator_producer_scope.py,
# test_drain_old_redis_bounded.py).

import ast
import asyncio
import contextlib
import hashlib
import inspect
import itertools
import os
import re
from collections.abc import AsyncIterator, Awaitable, Callable, Generator
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest

from taskq.settings import WorkerSettings
from taskq.testing.fixtures import (
    RUN_TOKEN_ENV_VAR,
    _schema_name_from_module,  # pyright: ignore[reportPrivateUsage]  # Why: shared test-infra naming helper under test; private prefix scopes it to the testing package (same pattern as _create_worker).
    _schema_name_from_test,  # pyright: ignore[reportPrivateUsage]  # Why: same as above.
    run_isolation_token,
)
from tests.conftest import (
    _call_window_leak_report,  # pyright: ignore[reportPrivateUsage]  # Why: shared test-infra helper under test; mirrors the conftest imports above.
    _fail_on_leaked_asyncio_tasks,  # pyright: ignore[reportPrivateUsage]  # Why: the autouse guard fixture under test; its raw async-gen function is driven manually below (a fixture cannot be re-entered through the real request).
    _leaked_pending_task_report,  # pyright: ignore[reportPrivateUsage]  # Why: shared test-infra helper under test; mirrors the conftest imports above.
    _module_db_name,  # pyright: ignore[reportPrivateUsage]  # Why: shared test-infra naming helper under test; mirrors tests/e2e's imports of conftest helpers.
    _reset_warn_once_stamps_reset,  # pyright: ignore[reportPrivateUsage]  # Why: the warn-once stamp reset under test; see the pin below.
    pytest_runtest_call,  # pyright: ignore[reportPrivateUsage]  # Why: the call-end snapshot hook under test; driven manually below.
)
from tests.test_rt_lost_job_soak import (
    _HANDBACK_BOUND_SECS,  # pyright: ignore[reportPrivateUsage]  # Why: soak budget constant under test; mirrors the conftest imports above.
    _ROUNDS,  # pyright: ignore[reportPrivateUsage]  # Why: same.
    _SETTLE_CAP_FLOOR_SECS,  # pyright: ignore[reportPrivateUsage]  # Why: same.
    _SETTLE_CAP_SECS_PER_JOB,  # pyright: ignore[reportPrivateUsage]  # Why: same.
    _STEP_BOUND_SECS,  # pyright: ignore[reportPrivateUsage]  # Why: same.
    _TERMINATE_RECONNECT_POLL_SECS,  # pyright: ignore[reportPrivateUsage]  # Why: same.
    _TERMINATE_RECONNECT_WINDOW_SECS,  # pyright: ignore[reportPrivateUsage]  # Why: same.
    stop_worker_and_reap_bootstrap,  # pyright: ignore[reportPrivateUsage]  # Why: same.
)
from tests.test_rt_lost_job_soak import (
    test_lost_job_soak_grand_mixin as _soak_grand_mixin,  # pyright: ignore[reportPrivateUsage]  # Why: aliased with the underscore so importing it does not collect the soak trial into this module's suite - only its timeout mark is read.
)

_TESTS_DIR = Path(__file__).parent
_SELF = Path(__file__)
_TESTING_PKG_DIR = _TESTS_DIR.parent / "src" / "taskq" / "testing"
# Worker-qualified-hash exception: the worker id is one hash input among
# several (module path / test node id), never the whole identifier.
_TESTING_PKG_WORKER_ALLOWLIST = frozenset({"fixtures.py"})
# tests/http_mock.py documents the retired bridge verbatim and is the one
# module allowed to drive respx directly, so it is excluded alongside _SELF.
_HTTP_MOCK = _TESTS_DIR / "http_mock.py"
# tests/_prom_review.py's port PARTITION (the prom-harness's per-worker
# band, 75b78c5f) is the worker-qualified-hash pattern's port twin: the
# worker id is one input to a bounded per-worker port range (gw<N> ->
# base + N * stride, modded within the stride), never a schema or an
# identifier — the conftest hash's sanction, at the port seam.
_PROM_REVIEW = _TESTS_DIR / "_prom_review.py"

_PYTEST_XDIST_WORKER_RE = re.compile(r"PYTEST_XDIST_WORKER")
_MODULE_SCHEMA_CONST_RE = re.compile(r"^_?SCHEMA\s*=", re.MULTILINE)


def _test_files() -> list[Path]:
    return [
        p
        for p in _TESTS_DIR.rglob("*.py")
        if p != _SELF and p.name != "conftest.py" and p != _PROM_REVIEW
    ]


def _testing_pkg_files() -> list[Path]:
    return [
        p for p in _TESTING_PKG_DIR.rglob("*.py") if p.name not in _TESTING_PKG_WORKER_ALLOWLIST
    ]


def _stack_scanned_files() -> list[Path]:
    return [p for p in _test_files() if p != _HTTP_MOCK]


def test_no_pytest_xdist_worker_derived_schema_names() -> None:
    """No test file may derive a schema/identifier name from
    ``PYTEST_XDIST_WORKER`` - it does not provide cross-file isolation
    within a worker (see module docstring). Use ``module_pg_schema`` /
    ``clean_pg_conn`` / ``clean_jobs_app`` or a unique per-test name
    instead.

    The allowlist covers NON-name uses only: ``_prom_review.py`` reads
    the worker id to PARTITION EPHEMERAL PORTS (each xdist worker's
    probes draw from their own 100-port band - the bind-close
    allocation raced at -n 8 and the loser's exporter died EADDRINUSE).
    A port is not a name: nothing persisted, nothing shared across
    files, no isolation claim - the worker id only de-conflicts
    simultaneous bind attempts within the one invocation. Any future
    use that touches a schema, a table, an identifier, or anything a
    database row remembers belongs NOWHERE in this list.
    """
    allowlist = {
        "_prom_review.py",  # port-band partition only (see above)
    }
    offenders = [
        str(p.relative_to(_TESTS_DIR))
        for p in _test_files()
        if p.name not in allowlist and _PYTEST_XDIST_WORKER_RE.search(p.read_text())
    ]
    assert not offenders, (
        "Found PYTEST_XDIST_WORKER-derived schema/name patterns in:\n"
        + "\n".join(f"  - {f}" for f in offenders)
        + f"\n\nUse the module_pg_schema / clean_pg_conn / clean_jobs_app fixtures, "
        "or a unique per-test name (e.g. f'prefix_{new_base62()}'), instead. "
        f"Non-name uses (ports) may join the allowlist WITH the justification: {sorted(allowlist)}."
    )


def test_testing_pkg_no_pytest_xdist_worker_derived_schema_names() -> None:
    """The PUBLISHED ``taskq.testing`` package must not derive names from
    ``PYTEST_XDIST_WORKER`` either - a fixed per-worker default there leaks
    into consumer suites on shared-database models. ``fixtures.py`` is
    allowlisted (worker-qualified hashes only - see module docstring).
    """
    offenders = [
        str(p.relative_to(_TESTING_PKG_DIR))
        for p in _testing_pkg_files()
        if _PYTEST_XDIST_WORKER_RE.search(p.read_text())
    ]
    assert not offenders, (
        "Found PYTEST_XDIST_WORKER-derived schema/name patterns in published "
        "taskq.testing package:\n"
        + "\n".join(f"  - {f}" for f in offenders)
        + "\n\nUse a per-call unique name (e.g. f'tq_{new_base62()}'.lower()), or a "
        "worker-qualified hash like fixtures.py's _schema_name_from_module, instead. "
        "A fixed per-worker name is shared by every caller in the process."
    )


def test_no_module_level_schema_constant() -> None:
    """No test file may define a module-level ``_SCHEMA`` / ``SCHEMA``
    constant. These tend to be shared (and stale) across many tests in
    a file; prefer fixture-derived or per-test-local schema names.
    """
    offenders = [
        str(p.relative_to(_TESTS_DIR))
        for p in _test_files()
        if _MODULE_SCHEMA_CONST_RE.search(p.read_text())
    ]
    assert not offenders, (
        "Found module-level _SCHEMA/SCHEMA constant(s) in:\n"
        + "\n".join(f"  - {f}" for f in offenders)
        + "\n\nUse module_pg_schema.schema_name (or a local per-test/per-call "
        "variable) instead of a module-level constant."
    )


def test_testing_pkg_no_module_level_schema_constant() -> None:
    """Same module-level ``_SCHEMA`` / ``SCHEMA`` ban for the published
    ``taskq.testing`` package (no allowlist - the pattern is never valid).
    """
    offenders = [
        str(p.relative_to(_TESTING_PKG_DIR))
        for p in _TESTING_PKG_DIR.rglob("*.py")  # no allowlist - never valid
        if _MODULE_SCHEMA_CONST_RE.search(p.read_text())
    ]
    assert not offenders, (
        "Found module-level _SCHEMA/SCHEMA constant(s) in published "
        "taskq.testing package:\n"
        + "\n".join(f"  - {f}" for f in offenders)
        + "\n\nUse a per-call unique name instead of a module-level constant."
    )


# ── taskq.testing submodule __all__ contracts ────────────────────────────
# The otel.py incident: the module's ``__all__`` was exactly inverted - it
# advertised six underscore-private helpers (the ``_save_*`` snapshots,
# ``_unpin_cached_loggers``, and the three autouse ``_*_guard`` fixtures)
# while omitting the public ``attribute_str``. ``fixtures.py`` had the same
# defect in miniature (``_create_worker`` listed; ``RUN_TOKEN_ENV_VAR`` /
# ``run_isolation_token`` / ``RedisContainerLike`` missing). No star-import
# consumer relied on the lie, so ``__all__`` itself was corrected; this pin
# holds the two contract-carrying testing modules to the rule so the surface
# cannot quietly rot again: ``__all__`` lists no underscore-private name and
# misses no public module-level definition.
#
# The scan is deliberately static (no module import): importing would make
# the pin's shape depend on the ``[otel]`` extra, and the existing guards
# above set the precedent of reading source, not process state.


def _testing_module_all_contract(module: str) -> tuple[set[str], set[str], set[str]]:
    """(public module-level names, declared ``__all__``) for one module.

    Public = ``def``/``class`` definitions and literal-constant assignments
    (``RUN_TOKEN_ENV_VAR = "..."``) that lack the underscore prefix. Imports
    are not public surface of the importing module - except when spelled in
    ``__all__`` as a deliberate re-export (fixtures.py re-exports nothing
    today, but the declared-side check below tolerates one). Top-level
    ``if``/``else`` branches are scanned one level deep so the runtime
    half of a ``if TYPE_CHECKING: ... else:`` split (fixtures.py's
    ``JobsApp`` / ``ModulePgSchema`` NamedTuples) counts as a definition;
    the typing-shim assignments in that same ``else`` branch
    (``WorkerDeps = ... = object``, a non-literal multi-target assign)
    fall outside "literal constant" and stay out of the contract.
    """
    path = _TESTING_PKG_DIR / f"{module}.py"
    tree = ast.parse(path.read_text())
    statements = [
        branch
        for node in tree.body
        for branch in ([node, *node.body, *node.orelse] if isinstance(node, ast.If) else [node])
    ]
    defined: set[str] = set()
    imported: set[str] = set()
    for node in statements:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            defined.add(node.name)
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and isinstance(node.value, ast.Constant):
                    defined.add(target.id)
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            defined.add(node.target.id)
        elif isinstance(node, (ast.Import, ast.ImportFrom)):
            imported.update((alias.asname or alias.name).split(".")[0] for alias in node.names)
    declared: set[str] | None = None
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == "__all__" for target in node.targets
        ):
            declared = set(ast.literal_eval(node.value))
    assert declared is not None, f"{path} does not declare __all__"
    publics = {name for name in defined if not name.startswith("_")} - {"__all__"}
    return publics, declared, imported


@pytest.mark.parametrize("module", ["fixtures", "otel"])
def test_testing_module_all_lists_public_surface_only(module: str) -> None:
    """A testing module's ``__all__`` IS its contract: it must contain
    every public module-level definition and no underscore-private name.

    Confusion prevented: a private name in ``__all__`` invites
    ``from taskq.testing.otel import _save_otel_enabled`` as sanctioned
    usage and lands the helper in rendered API docs, while a public name
    left out (``attribute_str``) is invisible to every tool that trusts
    the contract. Both directions of the lie are pinned here.
    """
    publics, declared, imported = _testing_module_all_contract(module)
    private_exported = sorted(name for name in declared if name.startswith("_"))
    public_missing = sorted(publics - declared)
    stale = sorted(declared - publics - imported)
    assert not private_exported, (
        f"taskq/testing/{module}.py __all__ exports underscore-private name(s) "
        f"{private_exported} - private helpers are not part of the module's "
        "contract; import them explicitly (with a reportPrivateUsage ignore) "
        "if you must reach them."
    )
    assert not public_missing, (
        f"taskq/testing/{module}.py omits public name(s) {public_missing} from "
        "__all__ - the contract must list the module's full public surface."
    )
    assert not stale, (
        f"taskq/testing/{module}.py __all__ lists name(s) {stale} that are "
        "neither public definitions nor imports of the module - stale entries."
    )


# ── Direct os.environ writes in the test tree ────────────────────────
# The atk_iso incident: a test wrote ``os.environ["TASKQ_SCHEMA_NAME"] = ...``
# directly (no restore on the failure path), and the value leaked into the
# CLI's settings load in a later test - the schema name is read lazily, so
# the write outlived the test that made it. ``monkeypatch.setenv`` is the
# sanctioned seam: it restores on every exit path, including teardown
# errors. For session-scoped fixtures, which have no ``monkeypatch``
# fixture of their own, the file-level pattern is a raw
# ``pytest.MonkeyPatch()`` instance + ``undo()`` (see ``DOTENV_DIR`` in
# tests/conftest.py) - still the sanctioned seam, never a bare
# ``os.environ`` subscript.
#
# Like every static scan here this has an honest limit: an alias
# (``env = os.environ``) or a ``globals()``/``setattr`` smuggle is
# invisible to a source scan. The suite's convention is direct spellings
# only; anything else is a review finding, not a pin.


def _all_test_tree_files() -> list[Path]:
    """Every file under tests/ - conftest.py files INCLUDED (unlike
    ``_test_files``): the env pin's whole point is that no file in the
    tree, fixtures included, mutates the process environment directly."""
    return [p for p in _TESTS_DIR.rglob("*.py") if p != _SELF]


def _dotted_name(node: ast.AST) -> str | None:
    """The dotted name a Name/Attribute chain spells out, else ``None``
    (calls, subscripts and other non-name bases cannot be resolved)."""
    parts: list[str] = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if isinstance(node, ast.Name):
        parts.append(node.id)
        return ".".join(reversed(parts))
    return None


_ENV_MUTATING_METHODS = frozenset({"pop", "popitem", "update", "setdefault", "clear"})


def _os_environ_direct_writes(tree: ast.Module) -> list[str]:
    """The source locations in *tree* that mutate ``os.environ`` directly.

    Covers the subscript store/del form, whole-object assignment and
    augmented assignment, the in-place dict methods, and the ``os.putenv``
    / ``os.unsetenv`` escape hatches. Pure reads (``Load`` context) are not
    writes and are ignored.
    """
    sites: list[str] = []
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Subscript)
            and isinstance(node.ctx, (ast.Store, ast.Del))
            and _dotted_name(node.value) == "os.environ"
        ):
            sites.append(f"line {node.lineno}: os.environ subscript write/del")
            continue
        if (
            isinstance(node, ast.Attribute)
            and isinstance(node.ctx, ast.Store)
            and _dotted_name(node) == "os.environ"
        ):
            sites.append(f"line {node.lineno}: os.environ assignment")
            continue
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            full = _dotted_name(node.func)
            if full == "os.environ" and node.func.attr in _ENV_MUTATING_METHODS:
                sites.append(f"line {node.lineno}: os.environ.{node.func.attr}()")
            elif full in {"os.putenv", "os.unsetenv"}:
                sites.append(f"line {node.lineno}: {full}()")
    return sites


def test_no_test_file_writes_os_environ_directly() -> None:
    """No file under tests/ may mutate ``os.environ`` directly - env writes
    go through ``monkeypatch.setenv`` / ``delenv`` (or, session-scoped, a
    raw ``pytest.MonkeyPatch`` instance with ``undo()``).

    A bare ``os.environ[...] = ...`` has no teardown: on the test's failure
    path the value stays in the process environment and leaks into every
    later reader - the atk_iso incident, where ``TASKQ_SCHEMA_NAME``
    survived into the CLI's settings load.
    """
    offenders: list[str] = []
    for path in _all_test_tree_files():
        for site in _os_environ_direct_writes(ast.parse(path.read_text())):
            offenders.append(f"{path.relative_to(_TESTS_DIR)}: {site}")
    assert not offenders, (
        "Found direct os.environ mutation(s) in the test tree:\n"
        + "\n".join(f"  - {f}" for f in offenders)
        + "\n\nUse monkeypatch.setenv/delenv (or, in a session fixture, a raw\n"
        "pytest.MonkeyPatch() instance with undo()) so every exit path restores\n"
        "the process environment. A bare os.environ write has no teardown and\n"
        "leaks into later tests' settings loads (the atk_iso incident)."
    )


# ── Process-global stdlib-module patching ────────────────────────────
# The asyncio.sleep incident: ``monkeypatch.setattr(run_mod.asyncio,
# "sleep", fake)`` reads as if it scoped the fake to ``run_mod``, but
# ``run_mod.asyncio`` is the global ``asyncio`` module - the fake lands on
# the process-global module, and every coroutine in the process that
# resolves ``asyncio.sleep`` during the patched window gets it, including
# module-scoped fixture machinery and any leftover task on the shared
# module loop. The failure then shows up in a later test - an ordering
# dependency, not a deterministic red.
#
# The fix this pin holds in place: patch the name where it is looked up -
# rebind the owning module's own ``<stdlib>`` binding to a delegation
# proxy with the fake shadowed (``tests/_ns_patch.py``), leaving the
# global module untouched for every other namespace.
#
# Two measured exemptions, both with no working module-local seam:
# - ``setattr(sys, "path"/"argv", ...)``: the import system and the CLI
#   argument vector are genuinely process-global state; there is nothing
#   module-local to rebind.
# - ``setattr(builtins, "__import__", ...)`` (the simulated-missing-extra
#   fakes in test_aws.py / test_aad.py): rebinding the module under
#   test's ``__builtins__`` to a copied dict does not scope the lookup on
#   CPython 3.13 - measured: the fake never fires for that module. The
#   fakes delegate to the real ``__import__`` for every name but the one
#   extra's module, and pytest runs tests single-threaded, so the
#   global-window hazard is not realized.


_STDLIB_MODULE_NAMES = frozenset(
    {"asyncio", "os", "time", "socket", "signal", "threading", "gc", "logging", "random"}
)
#: ``sys`` attributes with no module-local seam (see the section comment).
_SYS_SEAMLESS_ATTRS = frozenset({"path", "argv"})


def _global_stdlib_setattrs(tree: ast.Module) -> list[str]:
    """``setattr`` calls whose first argument resolves to (or reaches
    through to) a stdlib module object - the two spellings of the incident:
    ``setattr(asyncio, "sleep", ...)`` and the disguised
    ``setattr(run_mod.asyncio, "sleep", ...)``."""
    sites: list[str] = []
    for node in ast.walk(tree):
        if not (
            isinstance(node, ast.Call)
            and (
                (isinstance(node.func, ast.Attribute) and node.func.attr == "setattr")
                or (isinstance(node.func, ast.Name) and node.func.id == "setattr")
            )
            and node.args
        ):
            continue
        first = node.args[0]
        name = _dotted_name(first)
        if name is None:
            continue
        parts = name.split(".")
        last = parts[-1]
        penult = parts[-2] if len(parts) > 1 else None
        if last == "sys":
            # Exempt only the no-seam attributes; any other sys attr is
            # the incident's shape (the second arg is the attribute name).
            second_arg = node.args[1] if len(node.args) > 1 else None
            second = second_arg.value if isinstance(second_arg, ast.Constant) else None
            if second not in _SYS_SEAMLESS_ATTRS:
                sites.append(f"line {node.lineno}: setattr({name}, ...)")
            continue
        # ``builtins`` is exempt (see the section comment - no working
        # module-local seam, measured); ``sys`` only for its no-seam attrs.
        if last in _STDLIB_MODULE_NAMES or penult in _STDLIB_MODULE_NAMES:
            sites.append(f"line {node.lineno}: setattr({name}, ...)")
    return sites


def test_no_test_file_setattrs_a_global_stdlib_module() -> None:
    """No test may ``setattr`` a stdlib module OBJECT - neither bare
    (``setattr(asyncio, "sleep", ...)``) nor disguised as if scoped
    (``setattr(run_mod.asyncio, "sleep", ...)``: ``run_mod.asyncio`` IS the
    global module).

    The process-global patch is visible to every coroutine in the process
    for the length of the test - including module-scoped fixture machinery
    and leftover tasks on the shared module loop - so its failure mode is
    an ordering dependency in a LATER test (the asyncio.sleep incident).
    Patch where the name is LOOKED up instead: rebind the owning module's
    own ``<stdlib>`` binding to a delegation proxy with the fake shadowed
    (``tests/_ns_patch.py``).
    """
    offenders: list[str] = []
    for path in _all_test_tree_files():
        for site in _global_stdlib_setattrs(ast.parse(path.read_text())):
            offenders.append(f"{path.relative_to(_TESTS_DIR)}: {site}")
    assert not offenders, (
        "Found setattr() on a stdlib module object in the test tree:\n"
        + "\n".join(f"  - {f}" for f in offenders)
        + "\n\nThe first argument reaching a stdlib module object makes the patch\n"
        "process-global for the length of the test (setattr(run_mod.asyncio, ...)\n"
        "IS setattr(asyncio, ...)). Patch where the name is LOOKED UP instead:\n"
        "rebind the owning module's own <stdlib> binding to a delegation proxy\n"
        "with the fake shadowed - see tests/_ns_patch.py."
    )


# ── The per-module-database discipline (pg_container/pg_dsn) ─────────
# Every PG touch in the suite is scoped to the requesting module's own
# database: ``pg_dsn`` (tests/conftest.py) derives a per-module database
# name from the module path + run-isolation token, creates it on the
# invocation's one shared container, and ``drop database ... with
# (force)``s it on module teardown - so schemas, tables and rows a test
# leaves behind cannot outlive the module and cannot be observed by any
# other module, worker, or invocation. ``module_pg_schema`` /
# ``clean_pg_conn`` / ``clean_jobs_app`` layer per-module/per-test schema
# isolation on top of that database.
#
# The residue hazard is a test that reaches for the raw container DSN
# (the fixture ``pg_container``) without ``pg_dsn``: it would create
# schemas/tables/rows in the container's default database - shared with
# every module and worker of the invocation - where a fixed schema name
# collides across modules (the pre-per-module-database incident class)
# and leftover rows are observable state for whoever runs later. The
# only sanctioned direct consumer is the ``pg_dsn`` fixture itself.


def test_pg_container_is_only_consumed_through_pg_dsn() -> None:
    """No test-tree function may request the ``pg_container`` fixture
    without also requesting ``pg_dsn``.

    A bare ``pg_container`` consumer drives connections at the container's
    default database - shared state that survives the test (schemas, rows,
    roles) into every later module on the worker. Everything else in the
    tree connects through ``pg_dsn``'s per-module database, which module
    teardown force-drops.
    """
    offenders: list[str] = []
    for path in _all_test_tree_files():
        if path.name == "conftest.py":
            continue  # the pg_dsn fixture itself is the sanctioned consumer
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            if node.name == "pg_dsn":  # the fixture definition, wherever it lives
                continue
            args = [a.arg for a in node.args.args]
            if "pg_container" in args and "pg_dsn" not in args:
                offenders.append(
                    f"{path.relative_to(_TESTS_DIR)}: {node.name} (line {node.lineno})"
                )
    assert not offenders, (
        "Found pg_container consumer(s) without pg_dsn in the test tree:\n"
        + "\n".join(f"  - {f}" for f in offenders)
        + "\n\npg_container is the RAW container DSN - its default database is "
        "shared by every module and xdist worker of the invocation, so schemas "
        "and rows created there are cross-test residue. Take pg_dsn (the "
        "per-module database, force-dropped at module teardown) instead."
    )


# ── pg_stat_activity database scoping ────────────────────────────────
#
# pg_stat_activity is cluster-wide, and the invocation's one shared
# Postgres container hosts every xdist worker's per-module database (see
# the pg_dsn fixture's docstring): a lock-waiter gate that polls
# pg_stat_activity without a database scope counts backends it has no
# relationship with, and a statement-shape like cannot fix that - every
# bulk-cancel and deregister driving statement in the suite shares the
# ``matching as materialized`` shape, so another worker's parked drain
# satisfies the gate as surely as our own. A false positive lands the
# test's raced COMMIT early: test_rt_cancel_window_race.py's
# deregistration preflight then saw the claimed row as committed
# 'running' and refused with ActorHasActiveJobsError (a CI failure of the
# unscoped gate, mechanism reproduced deterministically against a two-database
# cluster), and test_rt_cancel_deadlock.py's holder would close the
# deadlock cycle before the drain's event INSERT parked, inverting which
# transaction's detector arms first. Every pg_stat_activity query in the
# test tree (and in the published testing package) must therefore scope
# to the querying connection's own database - ``datname =
# current_database()`` - the discipline the LISTEN-pid queries in
# test_stream.py / test_watch_reclaims.py already follow. Product-code
# queries are not this scan's business: they isolate by relation OID
# (regclass), which no other database's rows can match.


def _pg_stat_activity_string_constants(tree: ast.Module) -> list[str]:
    """Every non-docstring string value in *tree* that mentions
    pg_stat_activity. The parser folds implicit concatenation into one
    constant, so one constant is one query; an f-string folds to the
    concatenation of its literal chunks, with every chunk consumed by a
    fold suppressed from the standalone-constant pass (ast.walk yields a
    JoinedStr before its children, but only if the fold - not the walk -
    owns the recursion does a nested f-string avoid being re-emitted as
    a fragment). The scan therefore sees exactly the literal text of
    every query: a table name or scope predicate smuggled in through an
    interpolation or a variable is invisible to any static scan, which
    is the honest limit of a source pin. Docstrings are prose (this
    file's own included), not queries, and are skipped by node identity.
    """
    docstring_constant_ids: set[int] = set()
    docstring_joined_ids: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            body = node.body
            if body and isinstance(body[0], ast.Expr):
                first = body[0].value
                if isinstance(first, ast.Constant) and isinstance(first.value, str):
                    docstring_constant_ids.add(id(first))
                elif isinstance(first, ast.JoinedStr):
                    docstring_joined_ids.add(id(first))

    consumed: set[int] = set()
    seen_joined: set[int] = set()

    def _flatten(node: ast.JoinedStr) -> str:
        seen_joined.add(id(node))
        parts: list[str] = []
        for value in node.values:
            if isinstance(value, ast.Constant) and isinstance(value.value, str):
                consumed.add(id(value))
                parts.append(value.value)
            elif isinstance(value, ast.FormattedValue):
                inner = value.value
                if isinstance(inner, ast.JoinedStr):
                    parts.append(_flatten(inner))
                elif isinstance(inner, ast.Constant) and isinstance(inner.value, str):
                    consumed.add(id(inner))
                    parts.append(inner.value)
        return "".join(parts)

    joined: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.JoinedStr) and id(node) not in seen_joined:
            joined.append((id(node), _flatten(node)))

    queries: list[str] = []
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Constant)
            and isinstance(node.value, str)
            and id(node) not in docstring_constant_ids
            and id(node) not in consumed
            and "pg_stat_activity" in node.value
        ):
            queries.append(node.value)
    queries.extend(
        text
        for node_id, text in joined
        if node_id not in docstring_joined_ids and "pg_stat_activity" in text
    )
    return queries


def test_pg_stat_activity_queries_are_scoped_to_the_current_database() -> None:
    """No test may poll pg_stat_activity without a database scope - an
    unscoped query is satisfied by another xdist worker's parked backend
    on the shared cluster (see the section comment for the two failure
    modes that produced)."""
    files = [p for p in _TESTS_DIR.rglob("*.py") if p != _SELF]
    files.extend(_TESTING_PKG_DIR.rglob("*.py"))
    offenders: list[str] = []
    for path in sorted(files):
        for query in _pg_stat_activity_string_constants(ast.parse(path.read_text())):
            if "datname" not in query or "current_database" not in query:
                offenders.append(
                    f"{path.relative_to(_TESTS_DIR.parent)}: {query.strip().splitlines()[0]}"
                )
    assert not offenders, (
        "Found pg_stat_activity query(ies) without a datname = current_database() "
        "scope:\n"
        + "\n".join(f"  - {f}" for f in offenders)
        + "\n\npg_stat_activity is cluster-wide and the shared container hosts every "
        "xdist worker's database, so an unscoped query is satisfied by other "
        "workers' backends. Scope it to the querying connection's own database."
    )


# ── Two-HTTP-stack hygiene ──────────────────────────────────────────────
# The `taskq[oidc]` extra ships only httpx2 - nothing under src/taskq
# imports httpx - but the dev group installs httpx as well, so the test
# environment has two httpcore-backed stacks that cannot see each other's
# mocks: src/taskq/web/admin/auth/oidc.py fetches discovery and jwks over
# httpx2, and authlib's AsyncOAuth2Client (pinned >=1.8.0, which prefers
# httpx2 whenever it is importable) performs the token exchange. Stock
# respx patches httpcore only, so it sees one half. tests/http_mock.py
# registers a respx mocker targeting every installed httpcore instead;
# these guards keep the suite pointed at it.
#
# Three earlier guards, added when the OIDC suite still bridged the stacks,
# are retired here. Each docstring below records which one it replaces and
# why, so the protection moves rather than disappearing.

_STACK_BRIDGE_RE = re.compile(r"setattr\(\s*httpx2?\s*,\s*[\"'](?:Async)?Client[\"']")
_BARE_RESPX_RE = re.compile(r"@respx\.mock|respx\.mock\(")


def test_no_test_file_bridges_one_http_stack_to_another() -> None:
    """No test may rebind one stack's client class to the other's.

    Retires ``test_oidc_httpx2_bridge_is_still_present``, which asserted the
    OPPOSITE: it pinned tests/test_sso_oidc.py's ``monkeypatch.setattr(httpx2,
    "AsyncClient", httpx.AsyncClient)`` in place, because without it respx
    silently stopped intercepting the discovery and JWKS fetches. That bridge
    is gone - respx is now aimed at httpcore2 as well - and keeping it would
    mean every OIDC test ran against a client class production never
    constructs, hiding any httpx2-only difference in timeouts, redirects, TLS
    verification, proxies or exception types.
    """
    offenders = [
        str(p.relative_to(_TESTS_DIR))
        for p in _stack_scanned_files()
        if _STACK_BRIDGE_RE.search(p.read_text())
    ]
    assert not offenders, (
        "Found an httpx/httpx2 client-class bridge in:\n"
        + "\n".join(f"  - {f}" for f in offenders)
        + "\n\nSubstituting one stack for the other makes mocks apply while the "
        "test exercises a client production never builds. Use "
        "tests.http_mock.mock_http, which mocks every installed stack in place."
    )


def test_http_mocking_is_routed_through_the_multi_stack_helper() -> None:
    """No test may call respx directly; stock respx covers httpcore only.

    Retires ``test_respx_does_not_intercept_httpx2``, which pinned respx's
    blindness to httpx2 as a premise so the bridge stayed justified. That
    premise no longer holds: tests/http_mock.py registers a mocker whose
    targets include httpcore2, so respx CAN see httpx2 - but only when aimed
    through that helper. Guarding the entry point is what keeps the coverage.
    """
    offenders = [
        str(p.relative_to(_TESTS_DIR))
        for p in _stack_scanned_files()
        if _BARE_RESPX_RE.search(p.read_text())
    ]
    assert not offenders, (
        "Found a direct respx.mock() call in:\n"
        + "\n".join(f"  - {f}" for f in offenders)
        + "\n\nrespx's default mocker patches httpcore only, so httpx2 traffic "
        "escapes it unmocked. Use tests.http_mock.mock_http instead."
    )


def test_mock_http_intercepts_every_installed_stack() -> None:
    """Every installed stack must actually be intercepted, not just declared.

    Retires ``test_authlib_oauth_client_is_on_the_stack_respx_patches``, which
    asserted that authlib's ``AsyncOAuth2Client`` subclasses ``httpx.AsyncClient``
    so that respx would apply to the token exchange. That pinned the suite to
    the deprecated stack: authlib 1.8 prefers httpx2 whenever it is importable,
    and the correct upgrade would have failed that assertion for the right
    reason. Which stack authlib picks no longer matters - what matters is that
    every stack present is covered, which is checked here by making a real
    request on each one.
    """
    pytest.importorskip("respx")
    import importlib

    from tests.http_mock import installed_stacks, mock_http, stacks_for

    stacks = installed_stacks()
    assert "httpx2" in stacks, "httpx2 is expected in every CI leg via the dev group"

    url = "https://stack-coverage.test.invalid/probe"
    with mock_http() as router:
        router.get(url).mock(return_value=importlib.import_module("httpx").Response(200))
        for name in sorted(stacks):
            with importlib.import_module(name).Client() as client:
                assert client.get(url).status_code == 200, f"{name} was not intercepted"
        assert stacks_for(url) == set(stacks), (
            f"expected every installed stack {sorted(stacks)} to reach the mock, "
            f"got {sorted(stacks_for(url))}"
        )


# ── Run-isolation naming seam ──────────────────────────────────────
#
# Serial bare-``pytest`` runs share ``/tmp/pytest-of-<user>`` (the shared-pair
# state dir) across all invocations and checkouts, and the pre-token hash
# inputs were only ``(worker-or-"master", module)`` - invocation-invariant.
# Two overlapping serial runs of the same module therefore landed on the same
# database on the same shared pair, and each run's module teardown
# ``drop database ... with (force)`` killed the other run's live pools
# mid-test (redteam-reproduced: both runs rc=1 with pool-init failures on
# tq_db_5a19dc6e3f4c). The run token mixes the invocation-unique basetemp
# dir name into every hash input; under xdist the worker id already is
# invocation-unique, so behavior there is unchanged.


class _StubModuleRequest:
    """The ``request.module.__name__`` / ``request.node.nodeid`` surfaces the
    naming helpers read - lets the tests vary hash inputs without building a
    real ``FixtureRequest``."""

    def __init__(self) -> None:
        self.module = SimpleNamespace(__name__="tests.test_suite_hygiene")
        self.node = SimpleNamespace(nodeid="tests/test_suite_hygiene.py::test_stub")


def _legacy_module_db_hash(worker: str) -> str:
    """The pre-token hash of this module for *worker* - pins xdist inputs."""
    full = "tests_test_suite_hygiene"
    return "tq_db_" + hashlib.md5(f"{worker}_{full}".encode()).hexdigest()[:12]  # noqa: S324  # Why: mirrors the non-cryptographic naming hash under test.


def test_module_db_names_diverge_across_serial_run_tokens(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two concurrent serial runs of this module hash to DIFFERENT databases
    and schemas - the per-run token is part of every hash input."""
    monkeypatch.delenv("PYTEST_XDIST_WORKER", raising=False)
    stub = cast(pytest.FixtureRequest, _StubModuleRequest())
    db_names: set[str] = set()
    module_schemas: set[str] = set()
    test_schemas: set[str] = set()
    for token in ("pytest-41", "pytest-42"):
        monkeypatch.setenv(RUN_TOKEN_ENV_VAR, token)
        db_names.add(_module_db_name(stub))
        module_schemas.add(_schema_name_from_module(stub))
        test_schemas.add(_schema_name_from_test(stub))
    assert len(db_names) == 2, f"module database names collide: {db_names}"
    assert len(module_schemas) == 2, f"module schema names collide: {module_schemas}"
    assert len(test_schemas) == 2, f"test schema names collide: {test_schemas}"


def test_run_isolation_token_prefers_the_published_token(monkeypatch: pytest.MonkeyPatch) -> None:
    """The published run token wins over the xdist worker id - the conftest
    derives the token once at session start and the helpers read that seam."""
    monkeypatch.setenv(RUN_TOKEN_ENV_VAR, "pytest-41")
    monkeypatch.setenv("PYTEST_XDIST_WORKER", "gw3")
    assert run_isolation_token() == "pytest-41"


def test_xdist_hash_inputs_are_unchanged_by_the_token_seam(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Under xdist the hash inputs stay exactly the worker id - the token
    seam must not perturb per-worker names (their state dir is already
    per-invocation, so there is nothing to fix there). Covers both the bare
    worker env (direct library use) and the conftest-published token=gwK
    shape an actual xdist worker sees."""
    stub = cast(pytest.FixtureRequest, _StubModuleRequest())
    for with_published_token in (False, True):
        monkeypatch.delenv(RUN_TOKEN_ENV_VAR, raising=False)
        monkeypatch.setenv("PYTEST_XDIST_WORKER", "gw0")
        if with_published_token:
            monkeypatch.setenv(RUN_TOKEN_ENV_VAR, "gw0")
        assert _module_db_name(stub) == _legacy_module_db_hash("gw0")


def test_session_publishes_run_isolation_token(
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    """The session conftest publishes the token before any naming helper
    runs: the invocation-unique basetemp PATH (the numbered invocation dir
    PLUS the xdist worker's own subdirectory - invocation-unique and
    worker-distinct in one string, and distinct from any subprocess
    pytest's own fresh numbered dir - the mutual-drop class's root fix).
    The OLD token (the bare worker id, or the basetemp NAME alone) failed
    one of the two uniqueness legs: ``gw7`` is identical in every ``-n 8``
    invocation on the box, so a parent session and its own subprocess
    pytest (the scratch scenarios) hashed the same (token, module) pairs to
    the same database names on one cluster, and each side's
    ``DROP DATABASE ... WITH (FORCE)`` killed the other's live connections
    mid-test. The full path is unique per invocation, per worker, and per
    subprocess, by construction."""
    token = os.environ.get(RUN_TOKEN_ENV_VAR)
    assert token is not None, "session fixture did not publish the run token"
    assert token == str(tmp_path_factory.getbasetemp())


# ── Asyncio task-leak guard ────────────────────────────────────────
#
# conftest's _fail_on_leaked_asyncio_tasks fails any test that leaves an
# asyncio task pending on the module event loop - a live loop keeps
# writing shared state into later tests (module-scoped loops mean the
# task advances at every later test's await points). This pin holds the
# guard's classification to its contract: leaks are named (task name and
# coroutine), completed tasks and inherited baselines are not leaks.


def test_warn_once_stamp_reset_restores_the_global_stamps() -> None:
    """The ``_reset_warn_once_stamps`` autouse fixture's reset must clear
    every module's log-once stamps IN PLACE (importers may hold the dict
    by value) and must be idempotent across calls.

    The pollution class it closes: a test that drives a warned path
    stamps the process-global dict; a later warning-existence pin in the
    same worker finds its warning suppressed inside the 60s log window -
    an order-dependent red under pytest-randomly. If a fourth module
    grows a log-once stamp, add it to the fixture's reset and extend this
    pin; a stamp the reset misses reopens the class.
    """
    import taskq.progress._publish as publish_mod
    import taskq.worker._consumer as consumer_mod
    import taskq.worker.dispatch as dispatch_mod

    stamps = [
        dispatch_mod._reporter_defect_warned,  # pyright: ignore[reportPrivateUsage]  # Why: the module-global stamp under test; private prefix scopes it to the worker module.
        consumer_mod._dependency_failure_warned,  # pyright: ignore[reportPrivateUsage]  # Why: same.
        publish_mod._publish_failure_warned,  # pyright: ignore[reportPrivateUsage]  # Why: same.
    ]
    for stamp in stamps:
        stamp["probe-kind"] = 123.0
    _reset_warn_once_stamps_reset()
    for stamp in stamps:
        assert stamp.get("probe-kind") is None, (
            "the warn-once stamp reset left a stale stamp behind - the "
            "warning-existence pins are order-dependent again"
        )
    # In-place: the list above already holds the original dict objects
    # (no rebinding happened), which is the contract the fixture's
    # docstring promises value-importing callers.


def test_every_module_level_warn_once_stamp_is_covered_by_the_reset() -> None:
    """A fourth ``_*_warned`` stamp dict must not rely on human memory to
    join the reset. The hand-listed pin above can only red when one of the
    three KNOWN stamps survives the reset; a NEW module growing its own
    log-once stamp (``_foo_warned: dict[str, float] = {}`` beside a
    window constant) reopens the order-dependence class silently - the
    reset never hears about it. This pin discovers the stamp population
    from the source tree and behaviorally proves the reset clears each
    one, so the fixture's coverage grows when the production pattern does.

    Discovery shape: a module-level annotated assignment named
    ``_*_warned`` bound to an empty dict literal - the exact form the three
    window-gated warn sites share. A non-stamp that matches the name shape
    is cleared harmlessly (a fresh empty dict is the stamp dict's own
    reset semantics); if that is wrong for a future name, the failure
    message says exactly which candidate to rename or allowlist.
    """
    import importlib

    import taskq

    pkg_dir = Path(taskq.__file__).resolve().parent
    candidates: list[tuple[str, str]] = []
    for path in sorted(pkg_dir.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            target: ast.expr | None = None
            if isinstance(node, ast.AnnAssign):
                target = node.target
            elif isinstance(node, ast.Assign) and len(node.targets) == 1:
                target = node.targets[0]
            if not isinstance(target, ast.Name):
                continue
            name = target.id
            if not (name.startswith("_") and name.endswith("_warned")):
                continue
            if not isinstance(node, ast.AnnAssign | ast.Assign):
                continue
            value = node.value
            if isinstance(value, ast.Dict) and not value.keys:
                module_parts = path.relative_to(pkg_dir).with_suffix("").parts
                candidates.append((".".join(module_parts), name))

    discovered = [f"taskq.{module}.{name}" for module, name in candidates]
    assert len(discovered) >= 3, (
        f"only {discovered} module-level _*_warned stamps discovered (floor 3): "
        "the discovery walk has degraded and this pin proves nothing"
    )
    for module_name, attr in candidates:
        module = importlib.import_module(f"taskq.{module_name}")
        stamp = getattr(module, attr)
        assert isinstance(stamp, dict), f"taskq.{module_name}.{attr} is not a dict"
        stamp["probe-kind"] = 123.0
    _reset_warn_once_stamps_reset()
    survivors = []
    for module_name, attr in candidates:
        module = importlib.import_module(f"taskq.{module_name}")
        if getattr(module, attr).get("probe-kind") is not None:
            survivors.append(f"taskq.{module_name}.{attr}")
    assert not survivors, (
        f"module-level warn-once stamp(s) the reset never clears: {survivors} - a "
        "warning-existence pin against them is order-dependent again. Add them to "
        "_reset_warn_once_stamps_reset, or rename if the _*_warned shape is not a "
        "window-gated stamp."
    )


async def _hygiene_leak_probe_coro() -> None:
    await asyncio.Event().wait()


async def test_leaked_pending_task_report_names_the_leaked_task() -> None:
    """A pending task minted by the test is reported by name and coroutine;
    a task that completed and tasks pending since before the test are
    not leaks; cleaning the leak clears the report."""
    before = asyncio.all_tasks()
    done_task = asyncio.create_task(_hygiene_noop(), name="hygiene-done-probe")
    await done_task
    leaked_task = asyncio.create_task(_hygiene_leak_probe_coro(), name="hygiene-leak-probe")
    try:
        report = _leaked_pending_task_report(before, asyncio.all_tasks())
        assert report is not None, "a pending task minted by the test went unreported"
        assert "'hygiene-leak-probe'" in report
        assert "_hygiene_leak_probe_coro" in report
        assert "hygiene-done-probe" not in report, "a completed task is not a leak"
    finally:
        leaked_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await leaked_task
    assert _leaked_pending_task_report(before, asyncio.all_tasks()) is None


async def _hygiene_noop() -> None:
    return None


async def test_leaked_pending_task_report_treats_inherited_tasks_as_clean() -> None:
    """Tasks already pending when the test started (a module fixture's
    long-lived worker) are the test's inheritance, not its leak - the
    guard is a baseline-diff, so a long-lived task inherited and left
    running reports nothing."""
    inherited = asyncio.create_task(_hygiene_leak_probe_coro(), name="hygiene-inherited")
    try:
        before = asyncio.all_tasks()
        # Simulate a test that creates nothing and tears down cleanly: the
        # inherited task is still pending, but it was pending at baseline.
        assert _leaked_pending_task_report(before, asyncio.all_tasks()) is None
    finally:
        inherited.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await inherited


async def test_watchdog_trip_does_not_kill_the_test_process(
    intercepted_force_exits: list[tuple[str, str]],
) -> None:
    """A terminal watchdog trip in-process is a recorded event, not a dead
    pytest worker.

    The worker watchdog's contract is ``os._exit(EXIT_WATCHDOG)`` with the
    faulthandler dump on stderr - right for a production worker, fatal for
    the harness: in-process the "worker" IS the pytest worker, so a trip
    killed the whole xdist node mid-suite (xdist's
    ``node down: Not properly terminated``), xdist replaced it, and the
    leg dragged to its job cap with the trip's dump lost to the captured-
    output buffer a hard exit never flushes. The 3.12 leg of three separate
    CI runs died that way in one evening, at three different suite positions.

    The root conftest intercepts ``os._exit`` for the test process: the
    dump goes to fd 2, the trip is recorded, and ``SystemExit`` propagates
    in the calling thread instead. This pin proves the interception end to
    end with a deterministic injection - a real ``LoopLagWatchdog`` armed
    at a tiny budget and a deliberately blocked loop, no load lottery -
    and asserts the trip is both survivable and named.
    """
    import time as _time

    from taskq.worker._watchdog import LoopLagWatchdog, LoopLiveness

    del intercepted_force_exits[:]

    async def _blocked_run() -> None:
        loop = asyncio.get_running_loop()
        liveness = LoopLiveness()
        liveness.tick("hygiene-seam-probe", period=0.2)
        watchdog = LoopLagWatchdog(
            loop,
            liveness,
            budget=0.3,
            warn_budget=0.15,
            startup_grace=0.05,
            poll_interval=0.1,
        )
        watchdog.start()
        try:
            # The deterministic injection: a 0.8s loop stall, 2.6x the
            # budget - what CI co-tenancy produces by descheduling the
            # process. Without the seam this force-exits the process
            # (exit code 2, zero output); with it the trip is recorded
            # and the test keeps running.
            _time.sleep(0.8)  # noqa: ASYNC251  # Why: the blocking call is the injection - a deliberate loop stall the detector must catch.
        finally:
            watchdog.stop()

    await _blocked_run()

    assert intercepted_force_exits, (
        "the watchdog trip was not intercepted - the loop-lag detector "
        "never fired, so the seam's regression protection is vacuous"
    )
    code, site = intercepted_force_exits[-1]
    assert code == "2", f"the intercepted exit code is not EXIT_WATCHDOG: {code!r} at {site}"


async def _hygiene_window_probe_coro() -> None:
    await asyncio.sleep(3600)


async def test_call_window_leak_report_names_test_end_residue() -> None:
    """The call-window snapshot's contract: a task that was pending when
    the test's body finished is named - whether it is still pending by the
    guard's teardown check (the live diff's case) or finished during the
    teardown window (the shape the live diff cannot see: a worker bootstrap
    left mid-drain at test end, reaped by an awaiting teardown fixture —
    exactly the shape the leak report must land on the
    leaking test).  An empty window reports nothing."""
    window_dead: list[asyncio.Task[object]] = []
    finished = asyncio.create_task(_hygiene_noop(), name="hygiene-window-finished")
    await finished  # completed in-call: never pending at call end
    still = asyncio.create_task(_hygiene_window_probe_coro(), name="hygiene-window-still")
    reaped = asyncio.create_task(_hygiene_window_probe_coro(), name="hygiene-window-reaped")
    reaped.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await reaped
    # `reaped` is done now, but the snapshot model is: it was pending at
    # call end and done by teardown - so the pin feeds the done task list
    # the guard's teardown would hand the classifier.
    window_dead.append(reaped)
    try:
        report = _call_window_leak_report(window_dead)
        assert report is not None, "a task pending at call end, gone by teardown, went unreported"
        assert "'hygiene-window-reaped'" in report
        assert "pending at test end" in report
        # The live-diff case: the same guard names a task still pending.
        live_case = _leaked_pending_task_report(set(), asyncio.all_tasks())
        assert live_case is not None
        assert "'hygiene-window-still'" in live_case
    finally:
        still.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await still
    assert _call_window_leak_report([]) is None


# ── Guard wiring, end to end ────────────────────────────────────────
#
# The pins above drive the classifiers with hand-fed sets: deleting the
# pytest_runtest_call hook (or the guard's window wiring) leaves them
# green.  These pins drive the real hook and the real guard fixture
# through one simulated item lifecycle, so the wiring is under test: the
# hook must capture the call-end residue, and the guard's teardown must
# turn it into a failure on the leaking test.


class _GuardStubItem:
    """The minimal item the guard and the snapshot hook touch: the stash
    (the loop key at setup, the call-window key at call end) and the
    report sections (the guard's drained-leak report lands there)."""

    def __init__(self) -> None:
        self.stash = pytest.Stash()
        self.report_sections: list[tuple[str, str, str]] = []

    def add_report_section(self, when: str, key: str, content: str) -> None:
        self.report_sections.append((when, key, content))


class _GuardStubRequest:
    """The minimal request the guard fixture touches: ``node`` (whose
    stash carries the keys) and its own (unused) ``stash``."""

    def __init__(self) -> None:
        self.stash = pytest.Stash()
        self.node = _GuardStubItem()


_GUARD_RAW_FIXTURE = cast(
    "Callable[[_GuardStubRequest], AsyncIterator[None]]",
    _fail_on_leaked_asyncio_tasks._fixture_function,  # pyright: ignore[reportAttributeAccessIssue]  # Why: pytest 8+ wraps fixture functions in FixtureFunctionDefinition; the raw async-gen function is the only way to drive the fixture's setup/teardown by hand.
)


async def _drive_guarded_call(
    during_call: Callable[[], Awaitable[None]],
    during_teardown_window: Callable[[], Awaitable[None]] | None = None,
    during_setup: Callable[[], Awaitable[None]] | None = None,
) -> _GuardStubItem:
    """Run the REAL guard fixture and the REAL call-end snapshot hook
    through one simulated item lifecycle:

    guard setup (baseline) → *during_setup* (a function fixture's task,
    minted after the guard's baseline and before the call) → call-start
    snapshot → *during_call* (the test body) → call-end snapshot →
    *during_teardown_window* (the OTHER fixtures' teardowns, which run
    the loop and can reap residue) → guard teardown (raises the guard's
    failure on a leak the drain could not clean, ends the async generator
    cleanly otherwise).

    Raises whatever the guard's teardown raises (``pytest.fail.Exception``
    on a leak the drain could not clean; ``StopAsyncIteration`` on a
    clean loop) so each pin asserts the outcome itself. Returns the stub
    item: the drain's report sections (the drained leaks, named) are
    readable there.
    """
    request = _GuardStubRequest()
    item = request.node
    guard = _GUARD_RAW_FIXTURE(request)
    await guard.__anext__()  # setup: stash the loop, snapshot the baseline
    if during_setup is not None:
        await during_setup()
    hook = cast(
        "Generator[object, None, object]",
        pytest_runtest_call(cast(pytest.Item, item)),
    )
    next(hook)  # the call phase's call-start snapshot
    await during_call()
    with contextlib.suppress(StopIteration):
        hook.send(None)  # the call phase ends: the call-end snapshot lands
    if during_teardown_window is not None:
        await during_teardown_window()
    with contextlib.suppress(StopAsyncIteration):
        await guard.__anext__()  # teardown: the guard's check and verdict
    return item


async def test_guard_catches_residue_reaped_in_the_teardown_window() -> None:
    """The wiring pin for the call-window snapshot: a task minted by the
    test body, still pending when the body finished, and REAPED by a
    later fixture's teardown (the loop ran during teardown) must fail the
    LEAKING test - the live diff alone scores it green (done tasks are
    filtered), so deleting the snapshot hook or the guard's window wiring
    turns this pin red."""
    minted: list[asyncio.Task[object]] = []

    async def body() -> None:
        # Left pending on purpose: the simulated teardown window reaps it.
        minted.append(
            asyncio.create_task(_hygiene_window_probe_coro(), name="hygiene-wiring-residue")
        )

    async def teardown_window() -> None:
        residue = minted[0]
        residue.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await residue

    with pytest.raises(pytest.fail.Exception) as exc_info:
        await _drive_guarded_call(body, teardown_window)
    message = str(exc_info.value)
    assert "hygiene-wiring-residue" in message, (
        f"the teardown-window reap escaped the guard: {message}"
    )
    assert "pending at test end" in message


async def test_guard_drains_a_release_tail_that_outlives_the_call() -> None:
    """The CI leak class, allowed by name: a test body leaves a task
    holding a shielded slow release in flight (the asyncpg
    PoolConnectionHolder.release shape, marked with the ``release-tail:``
    name prefix), the call ends, and the guard's quiescence drain lets
    the release finish naturally. The test passes; the drained task is
    named in the report's warnings section, and no pending task outlives
    the drive."""
    release_done = asyncio.Event()
    minted: list[asyncio.Task[None]] = []

    async def _release_tail() -> None:
        await asyncio.sleep(0.01)
        release_done.set()

    async def body() -> None:
        async def _holder() -> None:
            await asyncio.shield(_release_tail())

        minted.append(asyncio.create_task(_holder(), name="release-tail:hygiene-pool-release"))

    item = await _drive_guarded_call(body)
    assert release_done.is_set(), "the drain must let the in-flight release finish"
    sections = [
        content for _, key, content in item.report_sections if key.startswith("leaked asyncio")
    ]
    assert sections and "release-tail:hygiene-pool-release" in sections[0], (
        f"the drained release tail must be named in the report: {item.report_sections}"
    )


async def test_guard_fails_a_detached_write_that_outlives_the_assertions() -> None:
    """The drain is not a pass for every leak: a task that writes shared
    state AFTER the test's assertions ran is the assertion-before-write
    class, and the guard fails the test even though the drain completed
    the task. A blanket drain-and-pass would hide exactly the late write
    the assertions did not see."""
    wrote_after_assertions = asyncio.Event()
    minted: list[asyncio.Task[None]] = []

    async def body() -> None:
        async def _late_write() -> None:
            await asyncio.sleep(0.05)
            wrote_after_assertions.set()

        minted.append(asyncio.create_task(_late_write(), name="detached-write"))
        # The pre-write assertion: the state the test saw. The write lands
        # after it, during the guard's drain.
        assert not wrote_after_assertions.is_set()

    with pytest.raises(pytest.fail.Exception) as exc_info:
        await _drive_guarded_call(body)
    assert wrote_after_assertions.is_set(), "the drain must have completed the late write"
    assert "detached-write" in str(exc_info.value), str(exc_info.value)


async def test_guard_drains_a_task_and_the_child_it_spawns() -> None:
    """The drain follows the spawned tree and the failure names it: an
    outliving task spawns a child during the drain's grace; both are
    completed, and because neither is a release tail the guard fails the
    leaking test and names the task it minted."""
    child_done = asyncio.Event()
    minted: list[asyncio.Task[None]] = []

    async def _child() -> None:
        await asyncio.sleep(0.005)
        child_done.set()

    async def _spawner() -> None:
        await asyncio.sleep(0.005)
        await asyncio.shield(_child())

    async def body() -> None:
        minted.append(asyncio.create_task(_spawner(), name="hygiene-spawner"))

    with pytest.raises(pytest.fail.Exception) as exc_info:
        await _drive_guarded_call(body)
    assert child_done.is_set(), "the drain must follow the spawned child to completion"
    assert "hygiene-spawner" in str(exc_info.value), str(exc_info.value)


async def test_guard_leaves_a_fixture_owned_task_alone() -> None:
    """A task a function fixture started (after the guard's setup, before
    the call) is in the call-start snapshot and the drain's ownership
    excludes it: the guard neither drains nor cancels it, and the test
    passes."""
    fixture_tasks: list[asyncio.Task[None]] = []

    async def _fixture_worker() -> None:
        await asyncio.sleep(3600)

    async def during_setup() -> None:
        fixture_tasks.append(asyncio.create_task(_fixture_worker(), name="hygiene-fixture-owned"))

    async def body() -> None:
        await asyncio.sleep(0.001)

    item = await _drive_guarded_call(body, during_setup=during_setup)
    assert fixture_tasks, "the pin's premise: the fixture task exists"
    assert not fixture_tasks[0].cancelled(), "the guard must not cancel a fixture's task"
    assert not fixture_tasks[0].done()
    sections = [
        content for _, key, content in item.report_sections if key.startswith("leaked asyncio")
    ]
    assert not sections, f"the fixture task must not be named as a leak: {sections}"
    fixture_tasks[0].cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await fixture_tasks[0]


async def test_guard_fails_a_survivor_within_the_drain_bound() -> None:
    """A task that ignores cancellation survives the drain: the guard
    fails the leaking test within the drain bound and names the
    survivor. The task stays stubborn past the drain's budget but exits
    on its own afterwards, so the module loop's teardown is not held."""
    import time as _time

    handles: list[asyncio.Task[None]] = []

    async def body() -> None:
        async def _stubborn() -> None:
            deadline = asyncio.get_running_loop().time() + 12.0
            while asyncio.get_running_loop().time() < deadline:
                try:
                    await asyncio.sleep(0.05)
                except asyncio.CancelledError:
                    continue

        handles.append(asyncio.create_task(_stubborn(), name="hygiene-survivor"))

    started = _time.monotonic()
    with pytest.raises(pytest.fail.Exception) as exc_info:
        await _drive_guarded_call(body)
    elapsed = _time.monotonic() - started
    assert "hygiene-survivor" in str(exc_info.value), str(exc_info.value)
    assert elapsed < 20.0, f"the drain bound did not hold: {elapsed:.1f}s"
    # The survivor dies on its own; await it so the module loop's
    # teardown starts clean.
    handles[0].cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await asyncio.wait_for(handles[0], timeout=15.0)


async def test_guard_catches_a_fixture_looking_task_name_smuggled_by_the_body() -> None:
    """The exemption is SET MEMBERSHIP (pending when the call phase
    started), never the task's NAME: a test body that mints a task with a
    fixture-looking name and abandons it is drained and named by the
    guard like any other leak. A name-based exemption would let a real
    leak sail past wearing a trustworthy label: the drained-leak report
    section must carry the smuggled task's name."""
    minted: list[asyncio.Task[object]] = []

    async def body() -> None:
        minted.append(asyncio.create_task(_hygiene_leak_probe_coro(), name="fixture-owned-fake"))

    with pytest.raises(pytest.fail.Exception) as exc_info:
        await _drive_guarded_call(body)
    smuggled = minted[0]
    assert smuggled.done(), "the drain must have completed the smuggled leak"
    message = str(exc_info.value)
    assert "fixture-owned-fake" in message, (
        f"a fixture-looking name smuggled a real leak past the guard: {message}"
    )


async def test_guard_passes_a_genuinely_fixture_owned_task() -> None:
    """The exemption's non-leak direction, through the real machinery: a
    task pending before the call phase (a function fixture's long-lived
    worker, minted at setup) is in the guard's baseline and the hook's
    call-start snapshot, so the guard scores the test green even though
    the task is still pending at teardown - its owner reaps it.  Without
    this direction the call-window snapshot would red every test that
    inherits a task from an async fixture."""
    fixture_owned = asyncio.create_task(
        _hygiene_leak_probe_coro(), name="hygiene-wiring-fixture-owned"
    )
    try:

        async def body() -> None:
            return None

        # No failure: the guard teardown ends the generator cleanly
        # (StopAsyncIteration), never pytest.fail.
        await _drive_guarded_call(body)
    finally:
        fixture_owned.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await fixture_owned


# ── The soak's teardown budget arithmetic ───────────────────────────
#
# The soak's outer 2700s pytest-timeout mark must cover the legitimate
# worst case including the two-stage teardown, and stage 1's stop bound
# must exceed the worker's own exit bound - the invariant that keeps the
# shutdown watchdog's os._exit (if it trips at all) inside stage 1's
# wait, so stage 2's reaping never feeds an armed watchdog.


def test_soak_stage1_stop_bound_exceeds_the_worker_exit_bound() -> None:
    """Stage 1's stop_timeout (120s) must exceed the worker's own exit
    bound: ``termination_grace_period`` (85s, counted from the shutdown's
    start) plus the exit tail (the dump-interval lag before the deadline
    trip is observed and the bounded metrics flush before os._exit, 8s as
    the settings property models it).  The bound is what makes the
    disarm-skip shape unreachable: a drain parked at the tracked-actor
    reap gate is bounded by the worker's own watchdog trip (whose
    ``os._exit`` lands inside stage 1's wait, killing a wedged process
    loudly); stage 2's awaits only run once the worker task is done and
    the watchdog disarmed.  If stage 1's bound ever dips below the
    worker's, a stage-1 timeout delivers a second cancellation into
    _main's cleanup finally - skipping ``shutdown_watchdog.cancel()`` and
    ``lag_watchdog.stop()`` (they sit after the gate await) - and stage 2
    runs with the watchdog armed: the os._exit lands mid-reaping, the CI
    EXIT=2 shape."""
    defaults = inspect.signature(stop_worker_and_reap_bootstrap).parameters
    stop_timeout = defaults["stop_timeout"].default
    reap_timeout = defaults["reap_timeout"].default
    settings = WorkerSettings.load_from_dict(
        {"pg_dsn": "postgresql://soak-budget.invalid/db", "schema_name": "soak_budget"}
    )
    worker_exit_bound = settings.termination_grace_period + settings.release_exit_tail_seconds
    assert stop_timeout > worker_exit_bound, (
        f"stage 1's stop bound ({stop_timeout}s) no longer exceeds the "
        f"worker's own exit bound ({worker_exit_bound}s = termination grace "
        f"{settings.termination_grace_period}s + exit tail "
        f"{settings.release_exit_tail_seconds}s): a wedged drain can now "
        "outlive stage 1, the timeout's second cancellation skips the "
        "watchdog disarm, and stage 2 reaps under an armed os._exit"
    )
    assert 0 < reap_timeout < stop_timeout, (
        "the residue reap must be bounded and strictly inside the stop "
        f"bound, got stop={stop_timeout}s reap={reap_timeout}s"
    )


def test_soak_worst_case_fits_the_timeout_mark() -> None:
    """The per-trial budget: the 2700s mark is PER ITEM (three trials are
    three items, each with its own budget), and each item's legitimate
    worst case - the body's bounds plus the finally's two-stage teardown
    (120s stop + 30s reap) - must fit inside it.  Bumping any constant
    that feeds this sum (the settle cap's per-job rate, the round count,
    a teardown bound) without re-checking the mark turns a worst-case
    failure into a pytest-timeout kill mid-teardown: the exact unbounded
    teardown this branch exists to prevent."""
    defaults = inspect.signature(stop_worker_and_reap_bootstrap).parameters
    stop_timeout = defaults["stop_timeout"].default
    reap_timeout = defaults["reap_timeout"].default

    slow_rounds = [r for r in range(_ROUNDS) if r % 40 == 11]
    kill_rounds = [r for r in range(_ROUNDS) if r % 33 == 5]
    job_count = 2 * _ROUNDS + len(slow_rounds)
    settle_cap = max(_SETTLE_CAP_FLOOR_SECS, _SETTLE_CAP_SECS_PER_JOB * job_count)
    body_worst = (
        2.0  # the bootstrap wait
        + _ROUNDS * 0.1  # the per-round pace sleep
        + len(kill_rounds) * (_TERMINATE_RECONNECT_WINDOW_SECS + _TERMINATE_RECONNECT_POLL_SECS)
        + settle_cap  # quiescence normally ends the settle far sooner
        + _STEP_BOUND_SECS  # the trial's worker-shutdown step (the outer bound)
        + _HANDBACK_BOUND_SECS
    )
    teardown_worst = stop_timeout + reap_timeout
    marks = {m.name: m for m in getattr(_soak_grand_mixin, "pytestmark", [])}
    mark_budget = marks["timeout"].args[0]
    total_worst = body_worst + teardown_worst
    assert total_worst < mark_budget, (
        f"the soak's worst case ({total_worst:.0f}s = body {body_worst:.0f}s "
        f"+ teardown {teardown_worst:.0f}s) no longer fits the "
        f"{mark_budget}s timeout mark: a worst-case trial now dies under "
        "pytest-timeout mid-teardown instead of through its own bounded "
        "watchdogs"
    )


# ── The soak's stage-2 residue reap, against its contract ───────────
#
# stop_worker_and_reap_bootstrap's stage 2 must hold its bound, name any
# survivor, surface a reaped task's real crash (a swallowed crash is a
# silent give-up: the doctrine is that a teardown that cannot finish is a
# finding), and stay silent when every reaped task honoured its
# cancellation.  The baseline is snapshotted immediately before minting,
# so the reap's residue is exactly what these pins mint.

_SOAK_REAP_PROBE_BOUND = 5.0
"""Wall-clock slack the stage-2 pin allows on top of its 0.5s reap bound."""


async def _soak_reap_swallower(release: asyncio.Event) -> None:
    """A residue task that absorbs one cancellation and re-parks: the
    shape the reap's loud failure exists for (a sibling that survives
    cancel+await).  Released by the pin's finally so it ends cleanly."""
    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        await release.wait()


async def _soak_reap_crasher() -> None:
    """A residue task that catches its cancellation and dies with a REAL
    exception instead - a bootstrap cleanup that raised."""
    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        raise ValueError("soak-reap-crasher boom") from None


async def test_soak_stage2_names_a_survivor_within_its_bound() -> None:
    """A residue task that swallows the cancellation survives the bounded
    await: stage 2 must fail LOUDLY, naming the survivor, and the bound
    must HOLD (the reap returns within reap_timeout + slack, not after
    some unbounded wait)."""
    baseline = frozenset(asyncio.all_tasks())
    worker_task = asyncio.create_task(_hygiene_noop(), name="soak-reap-done-worker")
    await worker_task
    release = asyncio.Event()
    swallower = asyncio.create_task(_soak_reap_swallower(release), name="soak-reap-swallower")
    # The residue stage's cancels land mid-await in the real bootstrap (a
    # wedged sibling is always mid-run): let the swallower reach its park
    # point, because a cancel delivered before a task's first step throws
    # into the coroutine at position 0 - before its try is entered - and
    # the task dies cancelled instead of swallowing.
    await asyncio.sleep(0)
    started = asyncio.get_running_loop().time()
    try:
        with pytest.raises(AssertionError) as exc_info:
            await stop_worker_and_reap_bootstrap(worker_task, baseline, reap_timeout=0.5)
        elapsed = asyncio.get_running_loop().time() - started
        assert elapsed < 0.5 + _SOAK_REAP_PROBE_BOUND, (
            f"the reap's bound did not hold: {elapsed:.1f}s"
        )
        message = str(exc_info.value)
        assert "TEARDOWN LEAK" in message
        assert "soak-reap-swallower" in message, f"the survivor was not named: {message}"
    finally:
        release.set()
        swallower.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await swallower


async def test_soak_stage2_surfaces_a_reaped_task_crash() -> None:
    """A residue task that dies with a REAL exception instead of the
    delivered cancellation is a bootstrap finding, not silence: stage 2
    must name the task and its exception.  The unguarded shape retrieved
    the exception and suppressed it - a crashing cleanup scored green."""
    baseline = frozenset(asyncio.all_tasks())
    worker_task = asyncio.create_task(_hygiene_noop(), name="soak-reap-done-worker")
    await worker_task
    crasher = asyncio.create_task(_soak_reap_crasher(), name="soak-reap-crasher")
    # Same mid-await discipline: the crasher must be parked in its try when
    # the reap's cancel arrives, so it can die with its real exception.
    await asyncio.sleep(0)
    try:
        with pytest.raises(AssertionError) as exc_info:
            await stop_worker_and_reap_bootstrap(worker_task, baseline, reap_timeout=0.5)
        message = str(exc_info.value)
        assert "TEARDOWN CRASH" in message
        assert "soak-reap-crasher" in message, f"the crashed task was not named: {message}"
        assert "soak-reap-crasher boom" in message, (
            f"the crash's exception was not surfaced: {message}"
        )
    finally:
        # The crasher is already done; nothing to reap. The assert above
        # re-raised if stage 2 stayed silent.
        _ = crasher.done()


async def test_soak_stage2_stays_silent_when_residue_honours_cancel() -> None:
    """The quiet direction: residue that honours its cancellation is
    reaped without a raise - the reap's red is for survivors and
    crashes, not for cancellation working as delivered."""
    baseline = frozenset(asyncio.all_tasks())
    worker_task = asyncio.create_task(_hygiene_noop(), name="soak-reap-done-worker")
    await worker_task
    honest = asyncio.create_task(_hygiene_leak_probe_coro(), name="soak-reap-honest")
    await stop_worker_and_reap_bootstrap(worker_task, baseline, reap_timeout=5.0)
    assert honest.cancelled() or honest.done()


# ── xdist_group pins: the measured state-affinity map ──────────────────────
#
# The five chaos modules share no single xdist_group: one shared group
# holds all 15 of their tests for a single worker as a convoy under
# --dist=loadgroup (measured: 62.1-72.6s of wall at -n 2 vs 46.1s
# split per-module (41 passed both ways). The db-global mechanisms that
# presumably motivated the shared group are all schema-qualified — the
# advisory locks go through schema_lock_name(...) on per-test-unique schemas
# (tc1_ .. tc5_{new_base62()}) and the NOTIFY channels are
# wake_channel(schema_name) — so each module now pins its own group and the
# pins below keep it that way.

_CHAOS_GROUP_MEMBERS: dict[str, str] = {
    "test_leader_chaos.py": "chaos_leader",
    "test_notify_chaos.py": "chaos_notify",
    "test_ratelimit_reservation_chaos.py": "chaos_ratelimit",
    "test_attack_livelock_pins.py": "chaos_livelock",
    "test_health_integration.py": "chaos_health",
}


def test_chaos_modules_carry_per_module_xdist_groups() -> None:
    """Every xdist_group in each chaos module must be exactly that module's
    own group — never a resurrected shared ``chaos`` group, never a stranger's
    name (loadgroup holds a group's tests for one worker: a shared name would
    re-form the convoy the split removed)."""
    seen: dict[str, str] = {}
    for filename, expected in _CHAOS_GROUP_MEMBERS.items():
        text = (Path(__file__).parent / filename).read_text(encoding="utf-8")
        groups = set(re.findall(r'xdist_group\(name="([^"]+)"\)', text))
        assert groups == {expected}, (
            f"{filename}: expected every xdist_group to be {expected!r} "
            f"(the per-module chaos split; see the tests/conftest.py docstring "
            f"for the schema-qualification that makes the split safe), "
            f"got {sorted(groups)}"
        )
        seen[filename] = expected
    # The split's whole point: no two chaos modules share a group name.
    assert len(set(seen.values())) == len(seen), f"two chaos modules share an xdist_group: {seen}"


def test_the_listen_group_stays_whole() -> None:
    """tests/web_admin/test_listen_coverage.py's ``listen`` group is the
    chaos family's one genuine db-global affinity: fixed LISTEN channel
    names on the shared pg_dsn, where two workers would receive each other's
    NOTIFYs (everything else in the family is schema-qualified). It must
    keep its group — whole and named ``listen``."""
    text = (Path(__file__).parent / "web_admin" / "test_listen_coverage.py").read_text(
        encoding="utf-8"
    )
    groups = re.findall(r'xdist_group\(name="([^"]+)"\)', text)
    assert groups and set(groups) == {"listen"}, (
        f"test_listen_coverage.py: the listen group moved, renamed or split: {groups}"
    )


# ── The mock-subtraction law, part 1: the fakes fail closed ──────────────
#
# A fake connection class whose fetch/fetchrow/fetchval/execute answers an
# unknown statement from a silent default (a literal, an unscripted
# ``self._x_result`` attribute, a sequential ``.pop(0)`` script) is a liar
# double: the recorded incident had the availability gate's probe eat a
# scripted batch and the sweeps deleted 0 of 7 — the probe matched nothing,
# the fake answered anyway, and every downstream assertion measured the
# answer, not the database. Two cures exist:
#
#   1. fail closed — the method's fall-through raises AssertionError naming
#      the statement fragment (every statement shape is a named match), and
#      the sequential pop(0) script dies with it; a named-subclass match
#      with an explicit raise-on-unmatched replaces it.
#   2. count, don't answer — where the property is statement
#      identity/order/count, the test wraps the real connection (the
#      recording-proxy pattern:
#      tests/test_cancel_where_bounded.py::_StatementRecordingPool); a fake
#      answers only for fault injection.
#
# A fake whose remaining purpose is fault injection (a branch no real
# database reaches deterministically — a driver raising mid-statement, a
# trigger swallowing RETURNING) stays exempt, but must say so: the class
# docstring carries the marker ``fail-closed-exempt:`` followed by the
# fault-injection subject it exists to inject. A marker without a subject
# is an offender like an unmarked class.

_FAIL_CLOSED_EXEMPT_MARKER = "fail-closed-exempt:"
_FAKE_CONN_METHOD_NAMES = frozenset({"fetch", "fetchrow", "fetchval", "execute"})


def _class_docstring(node: ast.ClassDef) -> str:
    body = node.body
    if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant):
        doc = body[0].value.value
        return doc if isinstance(doc, str) else ""
    return ""


def _is_default_answer_value(value: ast.AST) -> bool:
    """Whether *value* is the blind-default answer shape: a bare constant,
    an unscripted private-attribute answer (``self._x_result``), or the
    sequential pop(0) script's next page."""
    if isinstance(value, ast.Constant):
        return True
    if isinstance(value, ast.List) and not value.elts:
        return True  # the empty page the fake hands every unknown query
    if isinstance(value, ast.Attribute):
        return (
            isinstance(value.value, ast.Name)
            and value.value.id == "self"
            and value.attr.startswith("_")
        )
    if isinstance(value, ast.Call) and isinstance(value.func, ast.Attribute):
        # the sequential script: return self._results.pop(0)
        func = value.func
        return (
            func.attr == "pop"
            and isinstance(func.value, ast.Attribute)
            and isinstance(func.value.value, ast.Name)
            and func.value.value.id == "self"
            and func.value.attr.startswith("_")
        )
    return False


def _method_discriminates_on_sql(item: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
    """Whether the method's control flow BRANCHES on the statement text:
    the sql parameter (or a helper predicate called on it) participates in
    a comparison or a call — the shape that can answer unmatched SQL from
    a fall-through default. A method that never reads the statement (a
    pure recorder: record + return the tag) cannot lie per-statement."""
    params = {a.arg for a in item.args.args}
    sql_names = params & {"sql", "query", "statement", "q"}
    for n in ast.walk(item):
        # `if "fragment" in sql` / `sql == ...` / `sql.startswith(...)`
        if isinstance(n, ast.Compare):
            for side in (n.left, *n.comparators):
                for name in ast.walk(side):
                    if isinstance(name, ast.Name) and name.id in sql_names:
                        return True
        # `if _is_lease_statement(sql)` / `sql.strip().upper()...`
        if (
            isinstance(n, ast.Call)
            and isinstance(n.func, ast.Name)
            and "sql" in n.func.id.lower()
            and n.args
        ):
            for a in n.args:
                if isinstance(a, ast.Name) and a.id in sql_names:
                    return True
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute):
            base = n.func.value
            if isinstance(base, ast.Name) and base.id in sql_names:
                return True
    return False


def _fake_conn_default_answer_sites(tree: ast.Module) -> list[str]:
    """Every fake-class conn method that discriminates on the statement
    text and whose fall-through answers from a silent default: the
    method's final statement (the code an unmatched statement reaches)
    is a ``return`` of a default answer shape — a constant, an empty
    page, an unscripted private attribute, the next ``.pop(0)`` script
    page. A raise buried in a conditional arm does not excuse the
    fall-through: that raise fires only for the statements its arm
    names, and the fall-through still answers everything else (the
    recorded incident: the availability gate's probe matched nothing and
    the fall-through fed it the next scripted batch anyway). A method
    that never reads the statement text (a pure recorder — record +
    return the tag) cannot lie per-statement; it is not an offender.
    Returns offender descriptions."""
    offenders: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.ClassDef):
            continue
        for item in node.body:
            if not isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            if item.name not in _FAKE_CONN_METHOD_NAMES:
                continue
            if not _method_discriminates_on_sql(item):
                continue  # a pure recorder answers no "unmatched" SQL
            body = item.body
            if not body:
                continue
            last = body[-1]
            if not isinstance(last, ast.Return):
                continue  # the fall-through is not a return (a raise, a helper call)
            value = last.value
            if value is None or not _is_default_answer_value(value):
                continue
            offenders.append(
                f"{node.name}.{item.name} (line {item.lineno}): the fall-through "
                f"answers from a silent default"
            )
    return offenders


def _fail_closed_exempt_subject(doc: str) -> str:
    """The subject the class docstring's ``fail-closed-exempt:`` marker
    names, or ``""`` when the marker is absent or bare (nothing after
    it). The subject is the fault injection the fake exists to inject —
    a marker without one excuses nothing."""
    idx = doc.find(_FAIL_CLOSED_EXEMPT_MARKER)
    if idx < 0:
        return ""
    return doc[idx + len(_FAIL_CLOSED_EXEMPT_MARKER) :].strip()


def _fail_closed_offenders(tree: ast.Module) -> list[str]:
    """The tree's fail-closed offenders after the exempt adjudication: a
    site whose class docstring carries ``fail-closed-exempt:`` with a
    non-empty subject stays exempt; an unmarked class, or one carrying
    the bare marker, is an offender."""
    sites = _fake_conn_default_answer_sites(tree)
    if not sites:
        return []
    docstrings = {
        node.name: _class_docstring(node)
        for node in ast.walk(tree)
        if isinstance(node, ast.ClassDef)
    }
    offenders: list[str] = []
    for site in sites:
        class_name = site.split(".")[0]
        doc = docstrings.get(class_name, "")
        if _fail_closed_exempt_subject(doc):
            continue
        offenders.append(site)
    return offenders


def test_fake_connection_classes_fail_closed_on_unmatched_sql() -> None:
    """No fake class under tests/ may answer an unmatched statement from a
    silent default.

    A conn double (any class defining ``fetch``/``fetchrow``/``fetchval``/
    ``execute``) whose method's fall-through returns a literal, an
    unscripted ``self._x_result`` attribute, an empty page, or the next
    ``.pop(0)`` script is the shape that produced the probe-ate-a-batch
    incident (the availability gate's probe consumed a scripted batch; the
    sweeps deleted 0 of 7; every assertion downstream measured the fake's
    answer, not the database). The fall-through must RAISE naming the
    statement fragment, or the class docstring must carry
    ``fail-closed-exempt: <subject>`` naming the fault injection the fake
    exists to inject — a driver raising mid-statement, a trigger swallowing
    RETURNING — the branches no real database reaches deterministically.
    Where the property under test is statement identity/order/count, do not
    answer at all: wrap the real connection in a recording proxy
    (tests/test_cancel_where_bounded.py::_StatementRecordingPool).
    """
    offenders: list[str] = []
    for path in _all_test_tree_files():
        tree = ast.parse(path.read_text())
        offenders.extend(
            f"{path.relative_to(_TESTS_DIR)}: {site}" for site in _fail_closed_offenders(tree)
        )
    assert not offenders, (
        "Found fake conn class(es) answering unmatched SQL from a silent default:\n"
        + "\n".join(f"  - {f}" for f in offenders)
        + "\n\nThe fall-through must raise AssertionError naming the statement "
        "fragment (every shape a named match), or the class docstring must "
        "carry 'fail-closed-exempt: <subject>' naming the fault injection the "
        "fake exists to inject. Where the property is statement identity/order/"
        "count, wrap the real connection in a recording proxy instead "
        "(tests/test_cancel_where_bounded.py::_StatementRecordingPool) - the "
        "assertion counts real statements."
    )


# The fail-closed guard's adversarial self-checks: the guard's own walk,
# exercised on synthetic classes this pack carries as source strings (the
# guard scans this pack too, so the offenders must never exist as real
# class definitions).

_FAIL_CLOSED_SELF_SILENT_DEFAULT = '''
class _SyntheticSilentDefault:
    """Answers every unmatched statement from the next scripted page."""

    def __init__(self) -> None:
        self._results: list[list[object]] = []

    async def fetch(self, sql: str) -> list[object]:
        if "sweep" in sql:
            return [{"deleted": 7}]
        return self._results.pop(0)
'''

_FAIL_CLOSED_SELF_CONDITIONAL_RAISE_DEFAULT = '''
class _SyntheticConditionalRaiseDefault:
    """The shape that escaped the guard's first body: one arm raises,
    the fall-through still answers."""

    def __init__(self) -> None:
        self._results: list[list[object]] = []

    async def fetchrow(self, sql: str) -> object:
        if "availability_gate" in sql:
            raise AssertionError(f"unmatched statement: {sql}")
        return self._results.pop(0)
'''

_FAIL_CLOSED_SELF_FAIL_CLOSED = '''
class _SyntheticFailClosed:
    """Every shape a named match; the fall-through raises."""

    async def fetchrow(self, sql: str) -> object:
        if "hold" in sql:
            return {"hold_id": "h1"}
        raise AssertionError(f"unmatched statement: {sql}")
'''

_FAIL_CLOSED_SELF_BARE_EXEMPT = '''
class _SyntheticBareExempt:
    """fail-closed-exempt:"""

    def __init__(self) -> None:
        self._answer: object = None

    async def fetchval(self, sql: str) -> object:
        if "lease" in sql:
            raise RuntimeError("injected lease fault")
        return self._answer
'''

_FAIL_CLOSED_SELF_SUBJECT_EXEMPT = '''
class _SyntheticSubjectExempt:
    """fail-closed-exempt: the driver raises mid-statement — a branch no
    real database reaches deterministically."""

    async def execute(self, sql: str) -> str:
        if "swallow" in sql:
            raise ConnectionError("injected mid-statement drop")
        return "UPDATE 10"
'''


def test_fail_closed_guard_adversarial_self_check() -> None:
    """The guard's five verdicts on its own synthetic estate: the same
    AST walk the real guard runs must red the silent default, red the
    conditional-raise-plus-default shape, pass the fail-closed class,
    red the bare exempt marker, and pass the subject-carrying exempt."""
    silent = ast.parse(_FAIL_CLOSED_SELF_SILENT_DEFAULT)
    conditional = ast.parse(_FAIL_CLOSED_SELF_CONDITIONAL_RAISE_DEFAULT)
    closed = ast.parse(_FAIL_CLOSED_SELF_FAIL_CLOSED)
    bare = ast.parse(_FAIL_CLOSED_SELF_BARE_EXEMPT)
    subject = ast.parse(_FAIL_CLOSED_SELF_SUBJECT_EXEMPT)
    assert _fail_closed_offenders(silent), "(a) the silent-default fall-through must be an offender"
    assert _fail_closed_offenders(conditional), (
        "(b) the conditional-raise-plus-default shape must be an offender — "
        "the raise lives in one arm; the unmatched statement still reaches "
        "the answering fall-through"
    )
    assert not _fail_closed_offenders(closed), (
        "(c) the fail-closed class (its fall-through raises) must pass"
    )
    assert _fail_closed_offenders(bare), (
        "(d) a bare 'fail-closed-exempt:' marker with no subject must be an offender"
    )
    assert not _fail_closed_offenders(subject), "(e) a subject-carrying exempt marker must pass"


# The mock-subtraction law, part 2: the patch-site registry.
#
# Part A's mock subtraction cured the unit packs whose modules-under-test
# have real-PG sibling packs: the SQL-semantics arms died, and the
# survivors are fault injection and wiring the siblings do not own. The
# cured packs' — and only their — ``monkeypatch.setattr`` sites on
# ``taskq.*`` attributes live in the registry below: each attribute gets
# the reason it may stay patched (the real-PG sibling that owns the
# behavior, or why no sibling can). A new patch site in a registered pack
# must add its own entry with its own reason; the registry is the ledger,
# and the failure message points at it. A patch site in an unregistered
# pack is not this guard's business — whether that pack's module has a
# real-PG sibling is a human judgment the registry records when the pack
# is cured.

_TASKQ_PATCH_EXEMPT: dict[str, dict[str, str]] = {
    "test_leader.py": {
        "open_dedicated_conn": (
            "the dedicated-conn FACTORY seam: the unit pins the loop's wiring "
            "of the factory; the connections themselves are the real-PG packs' "
            "(tests/test_leader_prune.py, tests/test_postgres_sweeps.py)"
        ),
        "croniter": (
            "the cron CLOCK: the loops' fire cadence is time; the real-PG packs "
            "own what a fire DOES (tests/test_leader_prune.py), not when the "
            "schedule wakes"
        ),
        "_PRUNE_RETRY_BACKOFF_INITIAL_SECS": (
            "the retry ladder's tunable: the ladder's CADENCE is the unit's "
            "subject; the real-PG packs own the retried attempt's SQL"
        ),
        "_PRUNE_RETRY_BACKOFF_CAP_SECS": "same ladder tunable, the cap arm",
        "_otel_enabled": "the obs seam: the gauge/counter stamps are pinned on the OTel test stack (tests/test_obs.py), no PG involved",
        "_leader_lease_expires_in_seconds_cache": "same obs seam: the lease gauge's cached value",
        "asyncio": (
            "patched where LOOKED UP (tests/_ns_patch.py): the module's own "
            "asyncio binding, so the TEST's real sleeps keep their timing - "
            "the delegation-proxy pattern, not a behavior patch"
        ),
        "revert_stale_auto_disables": (
            "the takeover recovery's SEAM: the unit pins the assume-path's "
            "wiring; the recovery itself is the cron ownership pack's"
        ),
        "record_election_attempt": "the OTel counter's stamp site on the unit path; the counter's contract is tests/test_obs.py's",
        "CLOSE_TIMEOUT_SECS": (
            "the bounded-close deadline's TUNABLE: the hang/close arms drive "
            "the deadline deterministically on the fake clock; the close "
            "path's real behavior is tests/test_worker_deps_teardown.py's"
        ),
        "apply_keepalive_to_conn": (
            "the keepalive wiring's SEAM: the unit pins the factory path "
            "applies it; the socket work itself is the deps module's"
        ),
    },
    "test_cron_loop.py": {
        "resolve_payload": (
            "the payload-resolution SEAM: the unit pins the tick's planning "
            "arms around it; the resolution itself is the factories' unit pack"
        ),
        "time": "the tick's budget CLOCK: the deadline arithmetic is time; the fires' SQL is tests/test_cron_integration.py's",
        "record_cron_budget_deferral": "the deferral telemetry's seam: the wiring pin observes the call; the metric's contract is tests/test_obs.py's",
        "_otel_enabled": "the obs seam (no PG)",
        "croniter": "the schedule CLOCK (see test_leader.py's entry)",
        "compute_next_fire_after": "the fire computation's seam for the DST arms; the real DST behavior is tests/test_cron_integration.py's DST tests",
        "logger": "structlog capture seam: capture_logs cannot see through a frozen proxy; re-binding the module logger is the recorded workaround",
        "record_cron_failure": "the failure telemetry's seam: the wiring pins observe the emit; the counter's contract is tests/test_obs.py's",
    },
    "test_ratelimit_token_bucket.py": {
        "_acquire_pg": (
            "the BACKEND-DISPATCH isolation pin: backend='postgres' must reach "
            "_acquire_pg and never Redis - the dispatch shape is the unit's "
            "subject; the PG arm's behavior is tests/test_ratelimit_token_bucket_pg.py's"
        ),
        "_bucket_label_values": "the label-cap's cache reset (no PG; the cap's contract is tests/test_rt_worker_metric_cardinality.py's)",
    },
    "test_ratelimit_reservation.py": {
        "_probe_pg_lock_timeout_guc": "the lock-budget seam: the typed boundary arms; the real lock behavior is tests/test_rt_pools_ratelimit_lock_timeout_guc.py's",
    },
    "test_heartbeat.py": {
        "_tick_duration": (
            "the tick-completion WITNESS: the histogram's record call is the "
            "event the waits synchronize on (the house no-sleeps pattern); the "
            "histogram's own contract is tests/test_obs.py's"
        ),
        "update_heartbeat_consecutive_failures": "the failure-counter gauge's seam: the counter increments are the unit's subject (fault injection); the gauge's contract is tests/test_obs.py's",
        "_otel_enabled": "the obs seam (no PG)",
    },
    "test_shutdown_orchestrator.py": {
        "drain_local_queue_to_pending": (
            "the ladder's SCAFFOLDING: the phase-ladder arms (budgets, "
            "isolation, timeouts) need the ladder driven deterministically on "
            "the fake clock; the DRAIN itself is tests/test_shutdown_integration.py's "
            "(test_ti4_drain_to_pending, and the converted ordering pin "
            "test_the_ladder_stops_dispatch_before_the_hand_back)"
        ),
        "_METRICS_FLUSH_TIMEOUT_SECS": "the exit-tail constant the hold arithmetic reads (no PG; the bound's proof is test_suite_hygiene's own soak-budget pins)",
        "CLOSE_TIMEOUT_SECS": (
            "the bounded-close deadline's TUNABLE: the ladder's close arms "
            "drive the deadline deterministically; the close path's real "
            "behavior is tests/test_worker_deps_teardown.py's"
        ),
        "asyncio": "patched where LOOKED UP (tests/_ns_patch.py): the module's own asyncio binding - the delegation-proxy pattern, not a behavior patch",
    },
    "test_enqueue_coverage.py": {
        "NATIVE_TYPES": (
            "the record-shape fault injection: the enqueue paths' jsonb decode "
            "arms (a row the real driver cannot produce deterministically); "
            "the enqueue SQL's behavior is tests/test_postgres_enqueue.py's"
        ),
    },
    "test_postgres_dispatch.py": {},
    "test_dispatch_queue_mode_cache.py": {},
    "test_ratelimit_sliding_window.py": {},
    "test_ratelimit_refund_fix.py": {},
    "test_ratelimit_registry.py": {},
    "web_admin/test_ops_mutations.py": {
        "logger": "structlog capture seam (the audit-trail arms re-bind the module logger capture cannot see through)",
        "_SCHEDULE_RUN_COOLDOWN_SECONDS": (
            "the run-now cooldown's TUNABLE: the churn-bound arms drive the "
            "map's growth deterministically (no PG; the cooldown's own "
            "behavior is the ops route's)"
        ),
        "_last_schedule_run": (
            "the run-now cooldown map's STATE reset: the arms isolate the "
            "process-global map between tests (a per-test hygiene reset, "
            "not a behavior patch)"
        ),
        "_rate_limits": (
            "the registry's bucket MAP: the mutation arms seed/clear the "
            "in-process registry the route reads (the reset's real row "
            "behavior is tests/test_ratelimit_token_bucket_pg.py's "
            "test_pg_reset_deletes_the_bucket_row)"
        ),
        "_reservations": "same registry-state hygiene for the reservations page's arms",
        "reset": "the reset SEAM: the arm pins the route's delegation to the registry's reset; the reset's real behavior is the token-bucket _pg pack's",
        "peek_all": "the peek-all SEAM: the arm pins the route's error wrapping around the registry's own aggregate",
        "compute_next_fire_after": (
            "the skip/run-now fire computation's SEAM: the arms pin the "
            "route's wiring - the row's stored dst_strategy is what the "
            "route forwards, and a computation that never advances past "
            "the db clock (fault injection) must 400 before the skip "
            "UPDATE runs; the computation's own real behavior, the DST "
            "overlap included, is tests/test_cron_integration.py's DST "
            "tests (the unit arithmetic: tests/test_cron.py)"
        ),
        "resolve_payload": (
            "the payload-resolution FAULT-INJECTION seam: a factory "
            "raising (a wrong-type return, a generic error) is a branch "
            "the real factory path cannot reach deterministically; the "
            "arms pin the route's redirect-with-error-code wrapping "
            "around the raise, and the resolution's own behavior is "
            "tests/test_cron.py's resolve_payload unit pack"
        ),
        "sync_slots": (
            "the reservations page's sync SEAM: the success arm pins the "
            "page's re-fetch of the held/free rows after a successful "
            "sync, and the spy arm pins that foreign-schema reservations "
            "never reach the sync; the sync's real row behavior "
            "(reservation_slots insert/delete) is "
            "tests/test_ratelimit_reservation_pg.py's"
        ),
        "_fetch_redis_rl_state": (
            "the Redis-outage FAULT-INJECTION seam: the fetch returning "
            "None is the broker-down shape (no real Redis here) - the arm "
            "pins the page's visible degradation (redis_available flips "
            "back to False); the fetch's own contract (key conventions, "
            "decode, the one-pipeline round trip) is "
            "tests/web_admin/test_ops.py's fetch_redis_rl_state pack - "
            "no PG sibling can own a Redis read"
        ),
    },
}


def _taskq_module_aliases(tree: ast.Module) -> dict[str, str]:
    """Local names bound to taskq.* modules/classes by the file's imports."""
    aliases: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for a in node.names:
                if a.name.startswith("taskq"):
                    aliases[a.asname or a.name.split(".")[0]] = a.name
        elif isinstance(node, ast.ImportFrom) and node.module and node.module.startswith("taskq"):
            for a in node.names:
                aliases[a.asname or a.name] = f"{node.module}.{a.name}"
    return aliases


def _setattr_site_target(node: ast.Call, aliases: dict[str, str]) -> tuple[str, str, bool] | None:
    """The (taskq module path, attribute name, string-form flag) a
    ``setattr`` call patches, when statically resolvable; ``None``
    otherwise.

    pytest's two spellings name the same (module, attribute) pair and the
    guard must read both:

    * the object form - ``monkeypatch.setattr(module, "attr", value)`` -
      where the module comes from the file's ``taskq.*`` imports and the
      SECOND positional argument is the attribute-name literal;
    * the string form - ``monkeypatch.setattr("taskq.mod.attr", value)`` -
      where the FIRST argument is the fully-qualified dotted path and the
      second argument is already the replacement value: the attribute is
      the path's last component, the module everything before it.
    """
    args = node.args
    if len(args) < 2:
        return None
    target, second = args[0], args[1]
    if isinstance(target, ast.Name) and target.id in aliases:
        # The object form: the attribute name is the second argument's
        # string literal; the second argument is NOT the replacement here.
        if isinstance(second, ast.Constant) and isinstance(second.value, str):
            # ``import taskq.worker.leader as leader_mod`` aliases the
            # module; a ``from taskq.x import Thing`` alias binds a class
            # - the attribute is patched on the class whose module the
            # alias names.
            return aliases[target.id], second.value, False
        return None
    if isinstance(target, ast.Constant) and isinstance(target.value, str):
        dotted = target.value
        if not dotted.startswith("taskq"):
            return None
        module_path, _, attr = dotted.rpartition(".")
        if not module_path or not attr:
            return None
        return module_path, attr, True
    return None


def _taskq_patch_offenders(
    tree: ast.Module, aliases: dict[str, str], allowed: set[str]
) -> list[str]:
    """The tree's unregistered ``taskq.*`` patch sites, one description
    each; ``allowed`` is the pack's registered attribute names."""
    offenders: list[str] = []
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)):
            continue
        if not (node.func.attr == "setattr" and isinstance(node.func.value, ast.Name)):
            continue
        target = _setattr_site_target(node, aliases)
        if target is None:
            continue
        module_path, attr, string_form = target
        if attr in allowed:
            continue
        if string_form:
            offenders.append(
                f"line {node.lineno}: monkeypatch.setattr("
                f'"{module_path}.{attr}", ...) is unregistered'
            )
        else:
            offenders.append(
                f"line {node.lineno}: monkeypatch.setattr({module_path}, "
                f'"{attr}", ...) is unregistered'
            )
    return offenders


def test_taskq_patch_sites_in_the_cured_packs_are_registered() -> None:
    """Every ``monkeypatch.setattr`` on a ``taskq.*`` attribute inside a
    pack the mock-subtraction cured must carry a registry entry with its
    reason.

    The registry (:data:`_TASKQ_PATCH_EXEMPT`) is the ledger: each entry
    names the real-PG sibling pack that owns the patched attribute's
    behavior, or the reason no sibling can (fault injection, the obs
    seams, the delegation proxies). A NEW patch site in a registered pack
    turns this guard red until it adds its own entry - the message says
    exactly that. A patch site in an unregistered pack is not this guard's
    business: whether THAT pack's module has a real-PG sibling is the
    judgment the registry records when the pack is cured.
    """
    offenders: list[str] = []
    registry_names = set(_TASKQ_PATCH_EXEMPT)
    for name in sorted(registry_names):
        path = _TESTS_DIR / name
        if not path.exists():
            offenders.append(f"{name}: a registry entry names a file that does not exist")
            continue
        tree = ast.parse(path.read_text())
        aliases = _taskq_module_aliases(tree)
        for site in _taskq_patch_offenders(tree, aliases, set(_TASKQ_PATCH_EXEMPT[name])):
            offenders.append(f"{name}:{site}")
    assert not offenders, (
        "Found unregistered taskq.* patch site(s) in the mock-subtraction's "
        "cured packs:\n"
        + "\n".join(f"  - {f}" for f in offenders)
        + "\n\nThese packs' SQL-semantics arms were subtracted because real-PG "
        "sibling packs own the behavior; a patch site here must say why it may "
        "stay. Add the attribute to _TASKQ_PATCH_EXEMPT[<this pack>] with the "
        "reason naming the real-PG sibling that owns the attribute's behavior "
        "(or why no sibling can - fault injection, the obs seams, the "
        "delegation proxies) - or delete the patch and drive the real seam. "
        "Both pytest spellings count: the object form "
        'monkeypatch.setattr(module, "attr", ...) and the string form '
        'monkeypatch.setattr("taskq.mod.attr", ...) - in the string form the '
        "second argument is the replacement, not the name."
    )


# The registry guard's adversarial self-check: pytest's string spelling
# must resolve to the same (module, attribute) the object form names. The
# synthetic sites live as source strings (the guard scans this pack too,
# so the offenders must never exist as real call sites).

_PATCH_GUARD_SELF_STRING_FORM_UNREGISTERED = """
import pytest


def _synthetic_string_form_patch(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("taskq.web.admin.ops._synthetic_unregistered_attr", None)
"""

_PATCH_GUARD_SELF_STRING_FORM_REGISTERED = """
import pytest


def _synthetic_string_form_patch(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "taskq.web.admin.ops.compute_next_fire_after", lambda *args: None
    )
"""


def test_taskq_patch_guard_catches_the_string_form() -> None:
    """The guard's two verdicts on pytest's string spelling: a
    ``monkeypatch.setattr("taskq.mod.attr", value)`` site whose attribute
    is unregistered must red the same walk the object form feeds, and the
    same spelling over a registered attribute must pass."""
    unregistered_tree = ast.parse(_PATCH_GUARD_SELF_STRING_FORM_UNREGISTERED)
    registered_tree = ast.parse(_PATCH_GUARD_SELF_STRING_FORM_REGISTERED)
    assert _taskq_patch_offenders(unregistered_tree, {}, set()), (
        '(a) a string-form setattr("taskq.mod.attr", ...) over an '
        "unregistered attribute must be an offender"
    )
    registered_attrs = set(_TASKQ_PATCH_EXEMPT["web_admin/test_ops_mutations.py"])
    assert not _taskq_patch_offenders(registered_tree, {}, registered_attrs), (
        "(b) a string-form setattr over a registered attribute must pass"
    )
    # And the object form's verdicts are unchanged by the shared walk.
    object_form = ast.parse(
        "import pytest\n"
        "def _synthetic_object_form_patch(monkeypatch: pytest.MonkeyPatch) -> None:\n"
        "    monkeypatch.setattr(ops, '_synthetic_unregistered_attr', None)\n"
    )
    assert _taskq_patch_offenders(object_form, {"ops": "taskq.web.admin.ops"}, set()), (
        "(c) the object form over an unregistered attribute must still be an offender"
    )


# ── Long-running loop drivers go through the lifecycle supervisor ─────────
# The leak class CI caught twice: a test cancels a long-running loop
# mid-tick, detaching asyncpg's shielded PoolConnectionHolder.release onto
# the module loop. tests/_loop_control.managed_loop owns the start and the
# graceful stop for the modules on the supervisor list (the sweep-family
# drivers converted to it). The list grows as the remaining real-PG
# drivers convert (test_leader_integration.py, test_leader_chaos.py, the
# fleet and heartbeat integration modules); their leaked tasks are
# contained meanwhile by the conftest leak guard's teardown drain. The
# deliberate mid-tick cancel driver in test_leader_sweeps_coverage.py is
# the documented abrupt exemption: it cancels on purpose, over fakes, with
# no pool to leak, and stays off the list.

_LOOP_SUPERVISOR_MODULES = frozenset(
    {
        "test_leader_sweep_wiring_pg.py",
        "test_leader_sweep_reclaim_drain_wiring.py",
        "test_leader_sweep_rl_registry.py",
        "test_leader_sweep_keyed_reclaim_wiring.py",
        "test_leader_sweep_event_retention_wiring.py",
        "test_keyed_rate_limit_bucket_reclamation.py",
        "test_stale_batch_sweep_wiring.py",
    }
)

_REGISTERED_LOOPS = frozenset(
    {
        "_sweep_loop",
        "_scheduled_wake_loop",
        "_cron_loop",
        "_watchdog_loop",
        "_election_loop",
        "_prune_loop",
        "heartbeat_loop",
        "_health_check_loop",
        "notify_listener_loop",
    }
)


def _registered_loop_starts(path: Path) -> list[int]:
    """Line numbers of ``create_task`` calls that start a registered
    long-running loop (a named loop function, or a leader's ``run``)."""
    tree = ast.parse(path.read_text())
    starts = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        callee = getattr(node.func, "id", "") or getattr(node.func, "attr", "")
        if callee != "create_task" or not node.args:
            continue
        parts = set(re.split(r"[\s().,\[\]]", ast.unparse(node)))
        if parts & _REGISTERED_LOOPS:
            starts.append(node.lineno)
            continue
        target = node.args[0]
        if (
            isinstance(target, ast.Call)
            and isinstance(target.func, ast.Attribute)
            and target.func.attr == "run"
            and "leader" in ast.unparse(target).lower()
        ):
            starts.append(node.lineno)
    return starts


def _adjacent_set_then_cancel(path: Path) -> list[int]:
    """Line numbers of the legacy stop shape: a ``.set()`` call statement
    immediately followed by a ``.cancel()`` call statement."""
    tree = ast.parse(path.read_text())
    lines = []
    for node in ast.walk(tree):
        body = getattr(node, "body", None)
        if not isinstance(body, list):
            continue
        for first, second in itertools.pairwise(body):
            first_set = (
                isinstance(first, ast.Expr)
                and isinstance(first.value, ast.Call)
                and isinstance(first.value.func, ast.Attribute)
                and first.value.func.attr == "set"
            )
            second_cancel = (
                isinstance(second, ast.Expr)
                and isinstance(second.value, ast.Call)
                and isinstance(second.value.func, ast.Attribute)
                and second.value.func.attr == "cancel"
            )
            if first_set and second_cancel:
                lines.append(second.lineno)
    return lines


def test_long_running_loops_start_through_the_supervisor() -> None:
    """Every registered long-running loop start in a supervisor-module
    goes through tests/_loop_control.managed_loop, so no test there
    hand-rolls a task whose cancel can detach asyncpg's shielded release."""
    offenders = []
    for path in _test_files():
        if path.name not in _LOOP_SUPERVISOR_MODULES:
            continue
        for line in _registered_loop_starts(path):
            offenders.append(f"{path.relative_to(_TESTS_DIR)}:{line}")
    assert not offenders, (
        "Registered long-running loop(s) started outside "
        "tests/_loop_control.managed_loop:\n"
        + "\n".join(f"  - {f}" for f in offenders)
        + "\n\nStart them through managed_loop so the graceful stop, the "
        "grace bound, and the primary-exception preservation are owned in "
        "one place."
    )


def test_no_legacy_stop_shape_in_supervisor_modules() -> None:
    """No supervisor-module may hand-roll the legacy stop shape (a
    ``shutdown.set()`` immediately followed by a ``task.cancel()``): the
    adjacency is the mid-tick cancel that detaches asyncpg's shielded
    release. The supervisor's stop waits for the loop's own exit first."""
    offenders = []
    for path in _test_files():
        if path.name not in _LOOP_SUPERVISOR_MODULES:
            continue
        for line in _adjacent_set_then_cancel(path):
            offenders.append(f"{path.relative_to(_TESTS_DIR)}:{line}")
    assert not offenders, (
        "Legacy stop shape (shutdown.set() immediately followed by "
        "task.cancel()) in:\n"
        + "\n".join(f"  - {f}" for f in offenders)
        + "\n\nStop the loop through tests/_loop_control.managed_loop: "
        "shutdown first, a bounded grace, cancel only as a fallback."
    )

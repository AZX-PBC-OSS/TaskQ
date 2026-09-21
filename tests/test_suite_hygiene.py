"""Static hygiene guards for the test suite itself.

These are plain (non-PG, non-container) regression tests that grep the
``tests/`` tree AND the published ``src/taskq/testing/`` package for
anti-patterns which have previously caused cross-test and cross-worker
schema collisions under ``pytest-xdist``:

- ``os.environ.get("PYTEST_XDIST_WORKER", ...)``-derived schema names give
  NO real isolation - every test file within one xdist worker resolves to
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
ONE input to a per-module hash - the sanctioned worker-qualified-hash
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
# DELIBERATELY STATIC - codebase invariants with no runtime expression. A
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
#   - test_max_concurrent_docs_contract.py, and the README/deployment-guide
#     checks in test_migrate_on_start_worker.py - documentation contracts,
#     reading docs rather than source. Same shape as test_ci_workflow.py.
#   - this file - greps the TEST tree for anti-patterns, which is its job.
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
import os
import re
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest

from taskq.testing.fixtures import (
    RUN_TOKEN_ENV_VAR,
    _schema_name_from_module,  # pyright: ignore[reportPrivateUsage]  # Why: shared test-infra naming helper under test; private prefix scopes it to the testing package (same pattern as _create_worker).
    _schema_name_from_test,  # pyright: ignore[reportPrivateUsage]  # Why: same as above.
    run_isolation_token,
)
from tests.conftest import (
    _call_window_leak_report,  # pyright: ignore[reportPrivateUsage]  # Why: shared test-infra helper under test; mirrors the conftest imports above.
    _leaked_pending_task_report,  # pyright: ignore[reportPrivateUsage]  # Why: shared test-infra helper under test; mirrors the conftest imports above.
    _module_db_name,  # pyright: ignore[reportPrivateUsage]  # Why: shared test-infra naming helper under test; mirrors tests/e2e's imports of conftest helpers.
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

_PYTEST_XDIST_WORKER_RE = re.compile(r"PYTEST_XDIST_WORKER")
_MODULE_SCHEMA_CONST_RE = re.compile(r"^_?SCHEMA\s*=", re.MULTILINE)


def _test_files() -> list[Path]:
    return [p for p in _TESTS_DIR.rglob("*.py") if p != _SELF and p.name != "conftest.py"]


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
    """
    offenders = [
        str(p.relative_to(_TESTS_DIR))
        for p in _test_files()
        if _PYTEST_XDIST_WORKER_RE.search(p.read_text())
    ]
    assert not offenders, (
        "Found PYTEST_XDIST_WORKER-derived schema/name patterns in:\n"
        + "\n".join(f"  - {f}" for f in offenders)
        + "\n\nUse the module_pg_schema / clean_pg_conn / clean_jobs_app fixtures, "
        "or a unique per-test name (e.g. f'prefix_{new_base62()}'), instead."
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


# ── Direct os.environ writes in the test tree ────────────────────────
# The atk_iso incident: a test wrote ``os.environ["TASKQ_SCHEMA_NAME"] = ...``
# directly (no restore on the failure path), and the value leaked into the
# CLI's settings load in a LATER test - the schema name is read lazily, so
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
    ``_test_files``): the env pin's whole point is that NO file in the
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
# ``run_mod.asyncio`` IS the global ``asyncio`` module - the fake lands on
# the process-global module, and every coroutine in the process that
# resolves ``asyncio.sleep`` during the patched window gets it, including
# module-scoped fixture machinery and any leftover task on the shared
# module loop. The failure then shows up in a LATER test - an ordering
# dependency, not a deterministic red.
#
# The fix this pin holds in place: patch the name where it is LOOKED UP -
# rebind the owning module's own ``<stdlib>`` binding to a delegation
# proxy with the fake shadowed (``tests/_ns_patch.py``), leaving the
# global module untouched for every other namespace.
#
# Two measured exemptions, both with NO working module-local seam:
# - ``setattr(sys, "path"/"argv", ...)``: the import system and the CLI
#   argument vector are genuinely process-global state; there is nothing
#   module-local to rebind.
# - ``setattr(builtins, "__import__", ...)`` (the simulated-missing-extra
#   fakes in test_aws.py / test_aad.py): rebinding the module under
#   test's ``__builtins__`` to a copied dict does NOT scope the lookup on
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
    """``setattr`` calls whose FIRST argument resolves to (or reaches
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
    Patch where the name is LOOKED UP instead: rebind the owning module's
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
# Every PG touch in the suite is scoped to the requesting module's OWN
# database: ``pg_dsn`` (tests/conftest.py) derives a per-module database
# name from the module path + run-isolation token, creates it on the
# invocation's ONE shared container, and ``DROP DATABASE ... WITH
# (FORCE)``s it on module teardown - so schemas, tables and rows a test
# leaves behind cannot outlive the module and cannot be observed by any
# other module, worker, or invocation. ``module_pg_schema`` /
# ``clean_pg_conn`` / ``clean_jobs_app`` layer per-module/per-test schema
# isolation ON TOP of that database.
#
# The residue hazard is a test that reaches for the RAW container DSN
# (the fixture ``pg_container``) without ``pg_dsn``: it would create
# schemas/tables/rows in the container's DEFAULT database - shared with
# every module and worker of the invocation - where a fixed schema name
# collides across modules (the pre-per-module-database incident class)
# and leftover rows are observable state for whoever runs later. The
# ONLY sanctioned direct consumer is the ``pg_dsn`` fixture itself.


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
# pg_stat_activity is CLUSTER-wide, and the invocation's ONE shared
# Postgres container hosts every xdist worker's per-module database (see
# the pg_dsn fixture's docstring): a lock-waiter gate that polls
# pg_stat_activity without a database scope counts backends it has no
# relationship with, and a statement-shape LIKE cannot fix that - every
# bulk-cancel and deregister driving statement in the suite shares the
# ``matching AS MATERIALIZED`` shape, so another worker's parked drain
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
# The `taskq[oidc]` extra ships ONLY httpx2 - nothing under src/taskq
# imports httpx - but the dev group installs httpx as well, so the test
# environment has two httpcore-backed stacks that cannot see each other's
# mocks: src/taskq/web/admin/auth/oidc.py fetches discovery and JWKS over
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
# state dir) across ALL invocations and checkouts, and the pre-token hash
# inputs were only ``(worker-or-"master", module)`` - invocation-invariant.
# Two overlapping serial runs of the same module therefore landed on the SAME
# database on the SAME shared pair, and each run's module teardown
# ``DROP DATABASE ... WITH (FORCE)`` killed the other run's live pools
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
    runs: the xdist worker id under xdist, else the invocation-unique
    basetemp dir name (e.g. ``pytest-41``) - the value two overlapping
    serial runs can never share."""
    token = os.environ.get(RUN_TOKEN_ENV_VAR)
    assert token is not None, "session fixture did not publish the run token"
    worker = os.environ.get("PYTEST_XDIST_WORKER")
    expected = worker if worker is not None else tmp_path_factory.getbasetemp().name
    assert token == expected


# ── Asyncio task-leak guard ────────────────────────────────────────
#
# conftest's _fail_on_leaked_asyncio_tasks fails any test that leaves an
# asyncio task pending on the module event loop - a live loop keeps
# writing shared state into later tests (module-scoped loops mean the
# task advances at every later test's await points). This pin holds the
# guard's classification to its contract: leaks are NAMED (task name and
# coroutine), completed tasks and inherited baselines are not leaks.


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
            _time.sleep(0.8)  # noqa: ASYNC251  # Why: the blocking call IS the injection - a deliberate loop stall the detector must catch.
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
    the test's body finished is named - whether it is STILL pending by the
    guard's teardown check (the live diff's case) or finished during the
    teardown window (the shape the live diff cannot see: a worker bootstrap
    left mid-drain at test end, reaped by an awaiting teardown fixture,
    the exact shape run 35648058316's leak report demanded land on the
    leaking test).  An empty window reports nothing."""
    window_dead: list[asyncio.Task[object]] = []
    finished = asyncio.create_task(_hygiene_noop(), name="hygiene-window-finished")
    await finished  # completed in-call: never pending at call end
    still = asyncio.create_task(_hygiene_window_probe_coro(), name="hygiene-window-still")
    reaped = asyncio.create_task(_hygiene_window_probe_coro(), name="hygiene-window-reaped")
    reaped.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await reaped
    # `reaped` is done NOW, but the snapshot model is: it was pending at
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

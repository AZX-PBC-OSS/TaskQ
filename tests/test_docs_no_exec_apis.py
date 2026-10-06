"""API-existence pins for the docs' ``no-exec`` fences.

The docs-example harness (``tests/test_docs_examples.py``) executes every
bare ``​```python`` fence against the real package, so the RUNNABLE surface
cannot lie. The ``python no-exec`` fences are the gap: their taskq imports
and named APIs were never checked by anything, so a renamed export could
leave a doc's ``from taskq import X`` silently pointing at a name that no
longer exists.

This module closes that gap statically. Per documentation page, in fence
order, it accumulates the namespace the page builds — the exact contract
the fences' own reason strings claim (``names bound by an earlier
fence``) — and for every ``no-exec`` fragment enforces:

1. every ``from taskq... import NAME`` resolves against the live package
   (module importable, name present in the module's namespace or a
   submodule of it);
2. every ``import taskq...`` module is importable;
3. every dotted chain rooted at ``taskq`` (or at an alias bound to a
   taskq module by an earlier fence on the page — ``import taskq.retry
   as tr`` then ``tr.Delay(...)``) resolves attribute-by-attribute;
4. every name CALLED in the fragment that is a declared taskq export
   (``taskq.__all__`` / ``taskq.retry.__all__``) is bound — by an import
   in the fragment, an earlier fence on the page, or the fragment's own
   assignments — so an export can neither be renamed out from under a
   fragment nor silently dropped from the fence an earlier one leaned on.

Deliberate exemptions, each visible in the fence's own info string:
``no-exec — ... deliberately broken ...`` fences document paths that MUST
raise (the upgrading guide's old import paths) and are skipped entirely.
Fences that do not parse (signature excerpts written as pseudo-code) keep
enforcement 1 and 2 through a line-regex extraction of their imports;
rules 3 and 4 need a real AST and are skipped for exactly those fences.

The floors at the bottom are the anti-rot pins: a refactor that stops the
checker seeing fences or names fails here instead of the surface quietly
going dark (the same shape as ``test_docs_examples.py``'s own population
pins).
"""

from __future__ import annotations

import ast
import builtins
import importlib
import importlib.util
import re
import textwrap
from dataclasses import dataclass, field

import taskq
import taskq.retry
from tests._docs_examples import DocsExample, iter_examples

#: The declared export surface rule 4 checks free calls against: the two
#: modules the fences' import lines overwhelmingly name. Raw ``dir()`` is
#: deliberately NOT the surface — it includes transitive import artifacts
#: (``timedelta``, ``UUID``) that no doc should rely on unbound.
_DECLARED_EXPORTS = frozenset(taskq.__all__) | frozenset(taskq.retry.__all__)

_DELIBERATELY_BROKEN = "deliberately broken"

_BUILTINS = frozenset(dir(builtins))

_TASKQ_IMPORT_RE = re.compile(r"^\s*from (taskq(?:\.[A-Za-z_]\w*)*) import (.+?)\s*(?:#[^\"']*)?$")

#: Defensive floor, not a target: the population at introduction was
#: 236 no-exec fences across 26 pages (220 of them AST-parseable).
_NO_EXEC_FLOOR = 200
_PAGES_FLOOR = 20
#: Verified taskq-API references (rules 1-2) at introduction: 186 names.
_NAMES_FLOOR = 150


@dataclass
class _PageReport:
    """Per-run totals the floor pins assert against."""

    no_exec_checked: int = 0
    names_verified: int = 0
    pages: int = 0
    failures: list[str] = field(default_factory=list)


def _store_bound_names(tree: ast.AST) -> set[str]:
    """Every name a fence binds, however loosely scoped.

    Over-permissive on purpose: page-scope accumulation only needs the
    names a continuation fragment may LEGALLY reference; a name bound
    inside a nested function is a false-permissive answer for the page
    namespace, never a false red — the strict direction here would flag
    legitimate continuations and train readers to ignore the pin.
    """
    bound: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Store):
            bound.add(node.id)
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            bound.add(node.name)
        elif isinstance(node, ast.arg):
            bound.add(node.arg)
        elif isinstance(node, ast.alias):
            bound.add(node.asname or node.name.split(".")[0])
        elif isinstance(node, ast.ExceptHandler) and node.name:
            bound.add(node.name)
    return bound


def _regex_imports(code: str) -> list[tuple[str, str]]:
    """``(module, name)`` pairs a non-parseable fence imports, line-wise.

    For signature excerpts — the only fences that reach here — the import
    lines are plain single-line ``from taskq.x import Y`` statements, so
    the regex keeps enforcement 1 and 2 alive where the AST cannot.
    """
    pairs: list[tuple[str, str]] = []
    for line in code.splitlines():
        match = _TASKQ_IMPORT_RE.match(line)
        if match is None:
            continue
        module, names = match.groups()
        for name in names.split(","):
            name = name.strip().split(" as ")[0].strip()
            if name and name != "*":
                pairs.append((module, name))
    return pairs


def _taskq_from_imports(tree: ast.AST) -> list[tuple[str, str]]:
    """``(module, name)`` pairs from every ``from taskq... import ...``."""
    pairs: list[tuple[str, str]] = []
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.ImportFrom)
            and node.level == 0
            and node.module
            and (node.module == "taskq" or node.module.startswith("taskq."))
        ):
            for alias in node.names:
                if alias.name != "*":
                    pairs.append((node.module, alias.name))
    return pairs


def _taskq_plain_imports(tree: ast.AST) -> list[str]:
    """Every ``import taskq...`` module path spelled by the fence."""
    return [
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
        if alias.name == "taskq" or alias.name.startswith("taskq.")
    ]


def _dotted_chains(tree: ast.AST, module_aliases: dict[str, str]) -> list[tuple[str, str]]:
    """Dotted attribute chains rooted at ``taskq`` or a taskq-module alias.

    Returns ``(root_module, dotted_suffix)`` pairs: ``taskq.retry.Delay``
    yields ``("taskq", "retry.Delay")``; ``tr.Delay`` with
    ``module_aliases["tr"] == "taskq.retry"`` yields
    ``("taskq.retry", "Delay")``.
    """
    chains: list[tuple[str, str]] = []
    for node in ast.walk(tree):
        parts: list[str] = []
        cursor: ast.AST = node
        while isinstance(cursor, ast.Attribute):
            parts.append(cursor.attr)
            cursor = cursor.value
        if not isinstance(cursor, ast.Name) or not parts:
            continue
        if cursor.id == "taskq":
            chains.append(("taskq", ".".join(reversed(parts))))
        elif cursor.id in module_aliases:
            chains.append((module_aliases[cursor.id], ".".join(reversed(parts))))
    return chains


def _is_module(name: str) -> bool:
    try:
        return importlib.util.find_spec(name) is not None
    except (ImportError, ValueError):
        return False


def _resolve_chain(module: str, dotted: str) -> bool:
    """Does ``module.<dotted>`` resolve in the live package?

    Each path element is either an attribute of the object carried so
    far or a submodule reached through it.
    """
    obj: object = importlib.import_module(module)
    for part in dotted.split("."):
        if hasattr(obj, part):
            obj = getattr(obj, part)
        elif _is_module(f"{module}.{part}") and hasattr(type(obj), "__path__"):
            obj = importlib.import_module(f"{module}.{part}")
        else:
            return False
    return True


def _import_resolves(module: str, name: str) -> bool:
    """Does ``from module import name`` work today?"""
    mod = importlib.import_module(module)
    if hasattr(mod, name):
        return True
    return _is_module(f"{module}.{name}")


def _called_names(tree: ast.AST) -> set[str]:
    """Names in call position — ``Name(...)`` — the API-usage rule checks."""
    return {
        node.func.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }


def _verify_fragment(
    example: DocsExample,
    module_aliases: dict[str, str],
    page_bound: set[str],
    report: _PageReport,
) -> dict[str, str]:
    """Enforce the rules on one no-exec fence; return its namespace contributions.

    ``module_aliases`` maps alias → taskq module path for later fences'
    chain roots; ``page_bound`` accumulates every name an earlier fence
    bound. The contributions are merged by the caller after the fence is
    processed, so a fragment sees only what precedes it.
    """
    where = f"{example.example_id}"
    code = textwrap.dedent(example.code)
    try:
        tree = ast.parse(code)
    except SyntaxError:
        # Signature excerpts: keep rules 1-2 alive through the regex.
        for module, name in _regex_imports(code):
            report.names_verified += 1
            if not _import_resolves(module, name):
                report.failures.append(f"{where}: `from {module} import {name}` does not resolve")
        return {}

    contributions: dict[str, str] = {}
    for module, name in _taskq_from_imports(tree):
        report.names_verified += 1
        if not _import_resolves(module, name):
            report.failures.append(f"{where}: `from {module} import {name}` does not resolve")
        elif _is_module(f"{module}.{name}"):
            # The name IS a submodule (``from taskq import retry``): later
            # fences may chain attributes through it.
            contributions[name] = f"{module}.{name}"

    for module in _taskq_plain_imports(tree):
        report.names_verified += 1
        try:
            importlib.import_module(module)
        except ImportError as exc:
            report.failures.append(f"{where}: `import {module}` does not import ({exc})")

    for root_module, dotted in _dotted_chains(tree, module_aliases):
        report.names_verified += 1
        if not _resolve_chain(root_module, dotted):
            report.failures.append(
                f"{where}: `{root_module}.{dotted}` does not resolve against the live package"
            )

    if _DELIBERATELY_BROKEN not in example.info:
        bound_here = _store_bound_names(tree)
        for name in sorted(_called_names(tree) - bound_here - page_bound - _BUILTINS):
            if name in _DECLARED_EXPORTS:
                report.failures.append(
                    f"{where}: `{name}(...)` is called but never bound — not imported by "
                    "the fragment and not bound by an earlier fence on the page"
                )

    # Namespace contributions for later fences: every binding the fence
    # makes, plus the module aliases its taskq imports introduce.
    for bound in _store_bound_names(tree):
        page_bound.add(bound)
    for module, name in _taskq_from_imports(tree):
        if _import_resolves(module, name) and _is_module(f"{module}.{name}"):
            contributions[name] = f"{module}.{name}"
    for plain in _taskq_plain_imports(tree):
        head, _, rest = plain.partition(".")
        if rest:
            # ``import taskq.retry`` binds the ROOT name (``taskq``), not the
            # leaf; chains rooted there resolve natively, so record the head.
            contributions.setdefault(head, head)
    return contributions


def _check_pages() -> _PageReport:
    """Walk every page in fence order, accumulating the page namespace."""
    report = _PageReport()
    pages: dict[str, list[DocsExample]] = {}
    for example in iter_examples():
        pages.setdefault(example.path, []).append(example)
    report.pages = len(pages)

    for fences in pages.values():
        page_bound: set[str] = set()
        module_aliases: dict[str, str] = {}
        for example in fences:
            if example.executed:
                # The harness executes it; the namespace it binds is real.
                page_bound |= _store_bound_names(ast.parse(textwrap.dedent(example.code)))
                continue
            report.no_exec_checked += 1
            if _DELIBERATELY_BROKEN in example.info:
                continue  # documents a path that MUST fail; nothing to enforce
            contributions = _verify_fragment(example, module_aliases, page_bound, report)
            module_aliases.update(contributions)
    return report


def test_no_exec_fragments_taskq_apis_resolve() -> None:
    """Every taskq API a ``no-exec`` fence spells exists in the live package.

    The drift this closes: a renamed export used to leave the docs lying
    silently (only the RUNNABLE fences were ever executed). The failure
    below names each fence and the exact spelling that no longer
    resolves, so the fix is a docs edit, not an archaeology dig.
    """
    report = _check_pages()
    assert not report.failures, (
        "no-exec docs fences reference taskq APIs that do not resolve:\n"
        + "\n".join(f"  - {f}" for f in report.failures)
        + "\n\nFix the fence (update the name, or add the import the fragment's "
        "'names bound by an earlier fence' claim relies on), or tag it "
        "'no-exec — deliberately broken' if the fence documents a path that "
        "MUST fail (the upgrading guide's convention)."
    )


def test_no_exec_fragment_checker_population_has_not_collapsed() -> None:
    """The checker's surface is bounded below, so the pin cannot rot back
    to vacuous the way the prometheus selection did (0 tests, green)."""
    report = _check_pages()
    assert report.no_exec_checked >= _NO_EXEC_FLOOR, (
        f"only {report.no_exec_checked} no-exec fences checked "
        f"(floor {_NO_EXEC_FLOOR}): the extraction has degraded"
    )
    assert report.pages >= _PAGES_FLOOR, (
        f"only {report.pages} docs pages scanned (floor {_PAGES_FLOOR})"
    )
    assert report.names_verified >= _NAMES_FLOOR, (
        f"only {report.names_verified} taskq API references verified "
        f"(floor {_NAMES_FLOOR}): the enforcement surface has collapsed"
    )


def test_the_checker_reds_on_a_missing_export() -> None:
    """The checker is not vacuously green: fed a name the package does not
    export, it fails (this is the red-first proof, kept as a pin)."""
    missing = "NameTaskqHasNeverExported"
    assert missing not in _DECLARED_EXPORTS
    mod = importlib.import_module("taskq")
    assert not hasattr(mod, missing), "the probe name became real; rename the probe"
    assert not _import_resolves("taskq", missing)
    assert _import_resolves("taskq", "TaskQ"), "the live surface itself must resolve"


def test_the_checker_reds_on_an_unbound_called_export() -> None:
    """A fragment that CALLS a declared export bound nowhere reds, and one
    bound by an earlier fence passes — the namespace accumulation works."""
    fence_info = "python no-exec — not executed: fragment, names bound by an earlier fence"

    def _fragment(code: str, open_line: int) -> DocsExample:
        return DocsExample(
            example_id=f"scratch:{open_line}",
            path="scratch",
            open_line=open_line,
            close_line=open_line + 1,
            info=fence_info,
            code=code,
        )

    # Fragment 1 binds RetryPolicy (as an earlier fence would); fragment 2
    # calls it — the accumulated namespace keeps fragment 2 clean.
    binder = _fragment("from taskq import RetryPolicy\n", 1)
    caller = _fragment("RetryPolicy(max_attempts=1)\n", 10)

    report = _PageReport()
    page_bound = _store_bound_names(ast.parse(textwrap.dedent(binder.code)))
    _verify_fragment(caller, {}, page_bound, report)
    assert not report.failures, report.failures

    # Now the earlier fence's binding is gone: the call must red.
    report = _PageReport()
    _verify_fragment(caller, {}, set(), report)
    assert any("RetryPolicy(...)" in f for f in report.failures), report.failures

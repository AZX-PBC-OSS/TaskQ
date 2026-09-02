"""AST helpers for module-level import invariants.

These are genuine codebase invariants with no runtime expression — "the admin
package must not couple to ``taskq.worker`` at import time" is not something a
running system can be asked. So they stay static checks; what changes here is
the TOOL.

Substring greps over module source cannot see structure. They match the name in
a comment, a docstring or a string literal, and they cannot tell a module-level
import from one inside a function body — which is why the admin check had to
EXCLUDE ``ops.py`` entirely, losing the guarantee for that module rather than
expressing it. Parsing gives the real answer, so the exclusion goes away.
"""

from __future__ import annotations

import ast
import inspect
from functools import cache
from types import ModuleType


@cache
def _tree(module: ModuleType) -> ast.Module:
    return ast.parse(inspect.getsource(module))


def _imported_names(node: ast.Import | ast.ImportFrom) -> list[str]:
    if isinstance(node, ast.Import):
        return [a.name for a in node.names]
    if node.level:  # Why: a relative import cannot name an unrelated package.
        return []
    return [node.module] if node.module else []


def module_level_imports(module: ModuleType) -> set[str]:
    """Dotted module names imported at MODULE scope (not inside a def/class)."""
    return {
        name
        for node in _tree(module).body
        if isinstance(node, ast.Import | ast.ImportFrom)
        for name in _imported_names(node)
    }


def couples_to_at_import_time(module: ModuleType, package: str) -> list[str]:
    """Module-level imports of *package* or anything beneath it."""
    return sorted(
        name
        for name in module_level_imports(module)
        if name == package or name.startswith(f"{package}.")
    )


def has_future_annotations(module: ModuleType) -> bool:
    return any(
        isinstance(node, ast.ImportFrom) and node.module == "__future__"
        for node in _tree(module).body
    )


def imports_guarded_by_try(module: ModuleType) -> set[str]:
    """Modules imported inside a ``try`` anywhere in *module*.

    A dependency imported this way is optional by construction: the except arm
    decides what happens without it, and the module keeps importing.
    """
    guarded: set[str] = set()
    for node in ast.walk(_tree(module)):
        if not isinstance(node, ast.Try):
            continue
        for inner in ast.walk(node):
            if isinstance(inner, ast.Import | ast.ImportFrom):
                guarded.update(_imported_names(inner))
    return guarded

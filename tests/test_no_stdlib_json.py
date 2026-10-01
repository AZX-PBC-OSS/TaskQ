"""Pin the _json doctrine: the library never imports stdlib ``json`` directly.

``taskq/_json.py`` states the rule ("The library never imports stdlib
``json`` directly") and is the one sanctioned boundary over orjson. The
progress package already pins this per-module; this sweep holds the whole
``taskq`` package to the doctrine so no future module can drift back to the
stdlib import.

The sweep covers every import spelling AST can see: both ``import``
forms (plain, dotted, aliased), both ``from`` forms (top-level and
``json.decoder``-style submodule paths), and the dynamic escapes
``importlib.import_module("json")`` / ``__import__("json")`` (the
codebase legitimately calls ``import_module`` for actor modules, so only
a ``"json"`` literal argument is flagged). ``exec``/``eval`` of source
strings is out of scope: the AST no longer exists there to inspect.
"""

import ast
import pathlib

import taskq

_PACKAGE_ROOT = pathlib.Path(taskq.__file__).parent

# The one sanctioned boundary: this module wraps orjson and is the only
# place stdlib ``json`` may appear (its docstring names the rule, and
# orjson's JSONDecodeError subclasses json's).
_SANCTIONED = _PACKAGE_ROOT / "_json.py"


def test_no_stdlib_json_imports_outside__json() -> None:
    # Guard the guard: if ``taskq`` resolved somewhere without _json.py
    # (a stale site-packages shadow, an unexpected editable-install
    # layout), rglob would scan the wrong tree -- or nothing at all --
    # and this pin would pass vacuously. Fail loudly instead.
    assert _SANCTIONED.exists(), (
        f"expected the taskq package at {_PACKAGE_ROOT} to contain _json.py; "
        "the doctrine sweep is scanning the wrong tree"
    )
    violations: list[str] = []
    for path in sorted(_PACKAGE_ROOT.rglob("*.py")):
        if path == _SANCTIONED:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import) and any(
                alias.name == "json" or alias.name.startswith("json.") for alias in node.names
            ):
                violations.append(f"{path}:{node.lineno} import json")
            if isinstance(node, ast.ImportFrom) and (
                node.module == "json" or (node.module or "").startswith("json.")
            ):
                violations.append(f"{path}:{node.lineno} from {node.module} import ...")
            if isinstance(node, ast.Call) and (
                (isinstance(node.func, ast.Attribute) and node.func.attr == "import_module")
                or (isinstance(node.func, ast.Name) and node.func.id == "__import__")
            ):
                for arg in node.args:
                    if (
                        isinstance(arg, ast.Constant)
                        and isinstance(arg.value, str)
                        and (arg.value == "json" or arg.value.startswith("json."))
                    ):
                        violations.append(f"{path}:{node.lineno} dynamic import of {arg.value!r}")
    assert violations == [], (
        "taskq/_json.py's doctrine says the library never imports stdlib "
        f"json directly; found violations: {violations}"
    )

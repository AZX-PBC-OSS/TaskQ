"""Pin the _json doctrine: the library never imports stdlib ``json`` directly.

``taskq/_json.py`` states the rule ("The library never imports stdlib
``json`` directly") and is the one sanctioned boundary over orjson. The
progress package already pins this per-module; this sweep holds the whole
``taskq`` package to the doctrine so no future module can drift back to the
stdlib import.
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
            if isinstance(node, ast.ImportFrom) and node.module == "json":
                violations.append(f"{path}:{node.lineno} from json import ...")
    assert violations == [], (
        "taskq/_json.py's doctrine says the library never imports stdlib "
        f"json directly; found violations: {violations}"
    )

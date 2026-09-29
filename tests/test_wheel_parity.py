"""The wheel must ship exactly the source tree's importable surface.

Two independent halves, because either can fail alone:

1. CONTENTS parity: every ``.py`` file (and every non-``.py`` asset:
   migrations, templates, static files, ``py.typed``) under ``src/taskq/``
   appears in the built wheel, and the wheel ships nothing the source tree
   does not have. A module present in the source but missing from the wheel
   is an import error only for the pip-installed consumer — invisible to
   everyone who runs tests from a checkout.

2. IMPORT parity: every module the package ships imports cleanly in
   dependency order. A file can ship and still be import-broken (a module
   imported under a path outside the packaging list, a top-level import of
   an optional dependency without the guard), which no file-count check
   catches.

The wheel is built with the project's own toolchain (``uv build --wheel``)
into a temp dir; the build needs ``uv`` on PATH and is skipped otherwise.
"""

from __future__ import annotations

import importlib
import pkgutil
import re
import shutil
import subprocess
import zipfile
from collections.abc import Iterator
from pathlib import Path

import pytest

import taskq

_REPO_ROOT = Path(__file__).parent.parent
_SRC_PKG = _REPO_ROOT / "src" / "taskq"

# The documented missing-extra guard: an ImportError whose message names the
# extra with the PUBLISHED distribution name. This is the only import failure
# the install census tolerates (the dev env installs every extra, so a clean
# run has none; the tolerance keeps the census honest on bare-env CI legs).
_DOCUMENTED_GUARD = re.compile(r"taskq-py\[[a-z]+\]")


def _source_py_surface() -> set[str]:
    return {f"taskq/{path.relative_to(_SRC_PKG).as_posix()}" for path in _SRC_PKG.rglob("*.py")}


def _source_asset_surface() -> set[str]:
    return {
        f"taskq/{path.relative_to(_SRC_PKG).as_posix()}"
        for path in _SRC_PKG.rglob("*")
        if path.is_file() and path.suffix != ".py" and "__pycache__" not in path.parts
    }


@pytest.fixture(scope="session")
def built_wheel(tmp_path_factory: pytest.TempPathFactory) -> Path:
    if shutil.which("uv") is None:
        pytest.skip("uv is not on PATH; the wheel parity needs the project's own build tool")
    out_dir = tmp_path_factory.mktemp("wheel-parity")
    subprocess.run(  # noqa: S603  # Why: fixed literal argv — uv from PATH, the project's own build tool, no shell.
        ["uv", "build", "--wheel", "--out-dir", str(out_dir)],  # noqa: S607  # Why: uv resolved from PATH; fixed literal argv, no shell.
        cwd=_REPO_ROOT,
        check=True,
        capture_output=True,
    )
    wheels = sorted(out_dir.glob("*.whl"))
    assert len(wheels) == 1, f"expected exactly one built wheel, got {wheels}"
    return wheels[0]


def test_wheel_ships_exactly_the_source_surface(built_wheel: Path) -> None:
    with zipfile.ZipFile(built_wheel) as wheel:
        names = wheel.namelist()
    shipped_py = {n for n in names if n.startswith("taskq/") and n.endswith(".py")}
    shipped_assets = {n for n in names if n.startswith("taskq/") and not n.endswith(".py")}
    src_py = _source_py_surface()
    src_assets = _source_asset_surface()
    assert shipped_py == src_py, (
        f"module surface drift: wheel-only={sorted(shipped_py - src_py)} "
        f"source-only={sorted(src_py - shipped_py)}"
    )
    assert shipped_assets == src_assets, (
        f"asset drift (migrations/templates/static/py.typed): "
        f"wheel-only={sorted(shipped_assets - src_assets)} "
        f"source-only={sorted(src_assets - shipped_assets)}"
    )


def _all_shipped_modules() -> Iterator[str]:
    for mod in pkgutil.walk_packages(taskq.__path__, prefix="taskq."):
        yield mod.name
    # walk_packages skips nothing for a normal package, but the census must
    # not depend on that: every top-level module file is imported by name too.
    for path in sorted(_SRC_PKG.glob("*.py")):
        stem = path.stem
        if stem != "__init__":
            yield f"taskq.{stem}"


def test_every_shipped_module_imports() -> None:
    """Import each module the package ships; only documented extra guards may fail."""
    broken: list[str] = []
    for name in sorted(set(_all_shipped_modules())):
        try:
            importlib.import_module(name)
        except Exception as exc:
            if isinstance(exc, ImportError) and _DOCUMENTED_GUARD.search(str(exc)):
                continue
            broken.append(f"{name}: {type(exc).__name__}: {exc}")
    assert not broken, "modules the wheel ships that do not import:\n" + "\n".join(broken)

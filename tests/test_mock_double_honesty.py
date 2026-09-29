"""A spec'd Mock double must be honest about the interface it stands in for.

``test_double_signature_drift.py`` catches the signature drift of
monkeypatched function doubles. This file catches the OTHER double family:
``MagicMock(spec=X)`` / ``AsyncMock(spec=X)`` / ``Mock(spec=X)`` instances,
whose interface is pinned by *spec* — until the spec itself rots.

Two failure modes, both silent:

* **Spec rot** — the real class is renamed, moved, or the import alias
  changes meaning. The ``spec=`` keyword then receives whatever the name
  now binds (or the module dies at import and the test file's doubles
  degrade to untyped ``Any``), and every honesty guarantee the spec
  carried is gone while the tests stay green. A ``spec=`` that does not
  resolve to a class is the double-honesty equivalent of a scan of
  nothing.
* **Phantom configuration** — the test configures an attribute the real
  class does not have (``mock.attr = ...`` or ``mock.attr.return_value
  = ...``). ``spec=`` restricts *reads* of attributes the mock was born
  with, but an explicit assignment always succeeds: the mock silently
  ACQUIRES the phantom, the test's setup writes into a slot production
  never reads, and the pin passes against an interface that does not
  exist. A dataclass field is the canonical trap — ``WorkerDeps``
  declares ``settings``, ``worker_pool``, ``disowned_jobs`` etc. as
  fields, none of which exist as class attributes, so ``dir(cls)`` sees
  none of them and a config that misspells one (``disowned_job``) passes
  every run.

The scan walks the test tree, resolves every spec'd double's real class,
and checks each configured attribute against the class's HONEST
attribute set: class attributes/methods/properties (``dir``), dataclass
fields (``__dataclass_fields__``), slots, class-level annotations, and
the names ``__init__`` assigns (``self.name = ...`` — where
``asyncio.subprocess.Process.pid`` lives, invisible to ``dir``). The
list-spec form (``MagicMock(spec=["pubsub"])``) is checked against its
own list: configuring an attribute the list does not name is dead setup
the spec will refuse to serve.

Nothing here replaces the behavioural tests that use these doubles; it
is the automated drift catcher the signature file's docstring pointed at
and deferred — the row-shape half of the two quiet failure modes.
"""

from __future__ import annotations

import ast
import importlib
import inspect
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Final

_TESTS_DIR: Final = Path(__file__).resolve().parent

#: Spec'd doubles whose configured phantom attributes are deliberate.
#: Keyed by ``(file relative to tests/, the double's spec target, the
#: attribute name)``. Every entry needs a Why.
_PHANTOM_ALLOWED: Final[set[tuple[str, str, str]]] = set()

#: Below these floors the scan has degraded to scanning nothing (an AST
#: or import-alias regression in this file), and a scan of nothing always
#: passes. Measured on the tree this file shipped with.
_MIN_CLASS_SPECS: Final = 100
_MIN_LIST_SPECS: Final = 10


def _resolve(dotted: str) -> object | None:
    """Import *dotted*'s module and return the attribute it names."""
    module_path, _, attr = dotted.rpartition(".")
    while module_path:
        try:
            mod = importlib.import_module(module_path)
        except Exception:  # Why: an unimportable target is unresolvable here.
            module_path, _, head = module_path.rpartition(".")
            attr = f"{head}.{attr}" if head else attr
            continue
        obj: object = mod
        for part in attr.split("."):
            obj = getattr(obj, part, None)
            if obj is None:
                return None
        return obj
    return None


def _import_aliases(tree: ast.Module) -> dict[str, str]:
    """Map the names bound by imports to the dotted paths they refer to."""
    aliases: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for a in node.names:
                aliases[a.asname or a.name.split(".")[0]] = a.name
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            for a in node.names:
                aliases[a.asname or a.name] = f"{node.module}.{a.name}"
    return aliases


def _dotted_path(node: ast.expr, aliases: dict[str, str]) -> str | None:
    """Resolve an attribute/name expression to a dotted import path."""
    parts: list[str] = []
    cur = node
    while isinstance(cur, ast.Attribute):
        parts.append(cur.attr)
        cur = cur.value
    if not isinstance(cur, ast.Name):
        return None
    base = aliases.get(cur.id, cur.id)
    return ".".join([base, *reversed(parts)])


_SELF_ASSIGN_RE = re.compile(r"self\.([A-Za-z_][A-Za-z0-9_]*)\s*=")


def _honest_attributes(cls: type) -> frozenset[str]:
    """Every attribute name *cls* genuinely presents on an instance.

    ``dir(cls)`` covers class-level names but NOT dataclass fields without
    defaults (no class attribute is created), NOT slotted instance
    storage, and NOT names bound in ``__init__`` (where
    ``asyncio.subprocess.Process.pid`` lives). Each source is consulted
    separately; the union is the honest set.
    """
    honest: set[str] = set(dir(cls))
    fields = getattr(cls, "__dataclass_fields__", None)
    if fields:
        honest.update(fields)
    slots: set[str] = set()
    for klass in cls.__mro__:
        slots.update(getattr(klass, "__slots__", ()))
    honest.update(slots - {"__dict__", "__weakref__"})
    honest.update(getattr(cls, "__annotations__", {}))
    for klass in cls.__mro__:
        try:
            init = klass.__init__
            source = inspect.getsource(init)
        except (OSError, TypeError):
            continue
        honest.update(_SELF_ASSIGN_RE.findall(source))
    return frozenset(honest)


@dataclass(frozen=True, slots=True)
class _Finding:
    file: str
    lineno: int
    detail: str

    def describe(self) -> str:
        return f"  {self.file}:{self.lineno}  {self.detail}"


def _configured_attrs(tree: ast.Module, var: str) -> list[tuple[int, str]]:
    """Attribute names configured on *var* (stores and return_value /
    side_effect chains), with line numbers. Module-wide walk: a name is
    only included if it was bound to a spec'd mock constructor, and
    phantom detection is per (file, spec, attr), so cross-function reuse
    of a name cannot produce a false finding — only a wider net."""
    configured: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Attribute)
            and isinstance(node.value, ast.Name)
            and node.value.id == var
        ):
            if isinstance(node.ctx, ast.Store):
                configured.append((node.lineno, node.attr))
        elif (
            isinstance(node, ast.Attribute)
            and isinstance(node.ctx, ast.Store)
            and node.attr in ("return_value", "side_effect")
            and isinstance(node.value, ast.Attribute)
            and isinstance(node.value.value, ast.Name)
            and node.value.value.id == var
        ):
            # var.attr.return_value = x / var.attr.side_effect = x: the
            # chain configures `attr` (the tail is mock machinery), the
            # same surface the plain store above counts.
            configured.append((node.lineno, node.value.attr))
    return configured


def _scan() -> tuple[list[_Finding], int, int]:
    """Return (findings, class-spec count, list-spec count)."""
    findings: list[_Finding] = []
    class_specs = 0
    list_specs = 0
    for path in sorted(_TESTS_DIR.rglob("*.py")):
        if path == Path(__file__).resolve():
            continue
        try:
            tree = ast.parse(path.read_text())
        except SyntaxError:  # Why: a file that cannot parse is the parser's problem, not ours.
            continue
        aliases = _import_aliases(tree)
        rel = str(path.relative_to(_TESTS_DIR))
        for node in ast.walk(tree):
            if not (
                isinstance(node, ast.Assign | ast.AnnAssign) and isinstance(node.value, ast.Call)
            ):
                continue
            call = node.value
            if not (
                isinstance(call.func, ast.Name)
                and call.func.id in ("MagicMock", "AsyncMock", "Mock")
            ):
                continue
            spec_kw = next((k for k in call.keywords if k.arg == "spec"), None)
            if spec_kw is None:
                continue
            # Bound name(s) the configs are written against.
            if isinstance(node, ast.Assign):
                if len(node.targets) != 1 or not isinstance(node.targets[0], ast.Name):
                    continue
                var = node.targets[0].id
            else:
                if not isinstance(node.target, ast.Name):
                    continue
                var = node.target.id

            if isinstance(spec_kw.value, ast.List):
                # Attribute-allowlist form: the spec names exactly the
                # surface the double serves. A configured attribute the
                # list does not name is setup the spec will never serve.
                names = [
                    e.value
                    for e in spec_kw.value.elts
                    if isinstance(e, ast.Constant) and isinstance(e.value, str)
                ]
                if len(names) != len(spec_kw.value.elts):
                    continue
                list_specs += 1
                for lineno, attr in _configured_attrs(tree, var):
                    if attr not in names:
                        findings.append(
                            _Finding(
                                rel,
                                lineno,
                                f"{var}(spec=[{', '.join(names)}]).{attr} configured "
                                "but the spec list does not name it - dead setup the "
                                "spec refuses to serve; widen the list or drop the config.",
                            )
                        )
                continue

            dotted = _dotted_path(spec_kw.value, aliases)
            if dotted is None:
                continue
            cls = _resolve(dotted)
            if cls is None or not isinstance(cls, type):
                findings.append(
                    _Finding(
                        rel,
                        node.lineno,
                        f"{var} = {call.func.id}(spec={dotted}): the spec does not resolve "
                        "to a class - the double has silently lost every guarantee the "
                        "spec carried. Fix the import path or the double.",
                    )
                )
                continue
            class_specs += 1
            honest = _honest_attributes(cls)
            for lineno, attr in _configured_attrs(tree, var):
                if attr not in honest and (rel, dotted, attr) not in _PHANTOM_ALLOWED:
                    findings.append(
                        _Finding(
                            rel,
                            lineno,
                            f"{var}({call.func.id}(spec={dotted})).{attr} configured but "
                            f"{dotted} presents no such attribute (class surface, dataclass "
                            "field, slot, annotation, or __init__-assigned name) - the mock "
                            "silently acquires a phantom production never reads.",
                        )
                    )
        # create_autospec doubles enforce their own spec on attribute
        # access, but the class they name must still resolve: a rotted
        # path degrades the double the same way.
        for node in ast.walk(tree):
            if not (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == "create_autospec"
            ):
                continue
            if not node.args:
                continue
            dotted = _dotted_path(node.args[0], aliases)
            if dotted is None:
                continue
            # A bare local name that no import binds is a file-local spec
            # class (the create_autospec idiom: `_Methods` defined in the
            # test module) - its resolution is proven by the module
            # importing at all, and it is not drift-prone the way a
            # dotted third-party path is.
            if (
                "." not in dotted
                and node.args[0]
                and isinstance(node.args[0], ast.Name)
                and node.args[0].id not in aliases
            ):
                continue
            cls = _resolve(dotted)
            if cls is None or not isinstance(cls, type):
                findings.append(
                    _Finding(
                        rel,
                        node.lineno,
                        f"create_autospec({dotted}): the spec does not resolve to a class "
                        "- the double has silently lost every guarantee the spec carried.",
                    )
                )
            else:
                class_specs += 1
    return findings, class_specs, list_specs


def test_the_scan_resolves_enough_spec_doubles_to_be_meaningful() -> None:
    """A guard on the guard: an import-alias or AST regression could
    silently reduce this scan to zero doubles, and a scan of nothing
    always passes."""
    _findings, class_specs, list_specs = _scan()
    assert class_specs >= _MIN_CLASS_SPECS, (
        f"only {class_specs} class-spec'd doubles resolved (floor "
        f"{_MIN_CLASS_SPECS}): the honesty scan is no longer covering the "
        "suite - an import-alias or AST regression in this file, most likely"
    )
    assert list_specs >= _MIN_LIST_SPECS, (
        f"only {list_specs} list-spec'd doubles found (floor "
        f"{_MIN_LIST_SPECS}): same failure shape as the class-spec floor"
    )


def test_no_spec_double_resolves_to_a_missing_class() -> None:
    """Spec rot: the spec keyword must name a real, importable class."""
    findings, _cs, _ls = _scan()
    rot = [f for f in findings if "does not resolve" in f.detail]
    assert not rot, (
        "Spec'd Mock doubles whose spec no longer resolves to a class - the "
        "spec's guarantees are gone and the doubles test nothing:\n\n"
        + "\n".join(f.describe() for f in rot)
    )


def test_no_spec_double_configures_a_phantom_attribute() -> None:
    """Phantom configuration: every attribute configured on a spec'd
    double must exist on the real class it stands in for."""
    findings, _cs, _ls = _scan()
    phantoms = [f for f in findings if "does not resolve" not in f.detail]
    assert not phantoms, (
        "Spec'd Mock doubles configured with attributes the real class does "
        "not present - the setup writes into a slot production never reads:\n\n"
        + "\n".join(f.describe() for f in phantoms)
        + "\n\nFix the attribute name, or add the pair to _PHANTOM_ALLOWED "
        "with a Why: explaining why the phantom is deliberate."
    )

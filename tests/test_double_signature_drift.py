"""A monkeypatched test double must keep the signature it stands in for.

A double that outlives the signature of the real callable stops standing in for
anything. Two failure modes, and the quiet one is the dangerous one:

* **Hard drift** — the double demands arguments the real caller cannot supply,
  so every call raises ``TypeError``. Loud, but it surfaces as a confusing
  double failure instead of the assertion the test exists to make. Six of these
  shipped at once on this branch when ``_sse_generator`` lost an unused
  ``topic`` parameter and six stand-ins kept declaring it.
* **Soft drift** — the double is NARROWER than the real callable and survives
  only because production does not happen to pass the missing argument yet. It
  breaks on the day someone starts, and until then it silently under-models the
  thing it replaces.

The related hazard this file does NOT cover: a hand-rolled double (``FakeConn``,
``StubConnection``) declares no link to the type it imitates, so nothing static
can resolve it. Those drift by ROW SHAPE and RETURN TYPE rather than by
signature — a ``fetchval`` answering every statement with one canned value,
handed a bool where Postgres returns a datetime. Two of those also shipped on
this branch. They are caught by the tests that use them, loudly, and there is no
static equivalent; this module deliberately scopes itself to the resolvable case
rather than pretending to cover both.
"""

from __future__ import annotations

import ast
import importlib
import inspect
from dataclasses import dataclass
from pathlib import Path
from typing import Final

_TESTS_DIR: Final = Path(__file__).resolve().parent

#: Doubles that are deliberately narrower than the callable they replace.
#: Keyed by ``(test file relative to tests/, fully-qualified real symbol)``.
#: Every entry needs a Why: a reader must be able to tell "considered and
#: allowed" from "not yet detected".
_NARROWER_BY_DESIGN: Final[dict[tuple[str, str], str]] = {
    (
        "test_cli_credential_provider.py",
        "redis.asyncio.Redis.from_url",
    ): "Why: from_url takes the URL plus the whole Redis client kwarg surface. The "
    "double records url + kwargs and models none of them; these tests assert which "
    "credential path built the client, not how the client is configured.",
    (
        "test_cli_credential_provider.py",
        "asyncpg.create_pool",
    ): "Why: asyncpg.create_pool takes the DSN positionally plus ~25 connection "
    "kwargs. These tests assert which credential path built the pool, so the double "
    "records the kwargs it was given and models none of them; the CLI builds every "
    "pool with keyword arguments only.",
    (
        "test_cli_health.py",
        "asyncio.open_unix_connection",
    ): "Why: stdlib `limit` and **kwds are never passed by the CLI; the double takes "
    "the single positional path actually exercised.",
    (
        "test_heartbeat_isolate.py",
        "asyncpg.connect",
    ): "Why: asyncpg.connect declares 23 parameters. Modelling them to silence this "
    "scan would be noise, not safety; the double covers dsn + timeout, the whole "
    "surface the isolate path uses.",
    (
        "test_cli_actor_config.py",
        "taskq.cli.asyncpg.connect",
    ): "Why: as above — third-party breadth the CLI never uses.",
    (
        "test_cli_actor_deregister.py",
        "taskq.cli.asyncpg.connect",
    ): "Why: as above — third-party breadth the CLI never uses.",
    (
        "test_queue_ops_validation.py",
        "taskq.cli.asyncpg.connect",
    ): "Why: as above — third-party breadth the CLI never uses.",
    (
        "test_cli_worker.py",
        "taskq.cli.importlib.import_module",
    ): "Why: the CLI never passes `package`; the double takes the name it asserts on.",
    (
        "test_notify.py",
        "asyncio.sleep",
    ): "Why: stdlib `result` is never passed by TaskQ; the double takes the delay it asserts on.",
    (
        "test_leader_sweeps_coverage.py",
        "taskq.worker._leader_sweeps.asyncio.sleep",
    ): "Why: as above — a sleep accelerator only ever handed a delay.",
    (
        "test_shutdown_orchestrator.py",
        "taskq.worker.shutdown.asyncio.sleep",
    ): "Why: as above — a sleep accelerator only ever handed a delay.",
}


@dataclass(frozen=True, slots=True)
class _Params:
    """The shape of a signature, reduced to what drift depends on."""

    names: tuple[str, ...]
    positional: int
    required_kwonly: frozenset[str]
    accepts_var_positional: bool
    accepts_var_keyword: bool
    #: Positional-or-keyword parameter names, in order, that have no default.
    required_positional_names: tuple[str, ...]

    def required_names_beyond(self, index: int) -> set[str]:
        """Required positional parameters a caller can no longer fill
        positionally, because *index* slots is all it has."""
        return set(self.required_positional_names[index:])


def _params_from_signature(fn: object) -> _Params | None:
    try:
        sig = inspect.signature(fn)  # type: ignore[arg-type]  # Why: callability is checked by the caller.
    except (TypeError, ValueError):
        return None
    names: list[str] = []
    required_pos_names: list[str] = []
    positional = 0
    required_kwonly: set[str] = set()
    var_pos = var_kw = False
    for p in sig.parameters.values():
        if p.name == "self":
            continue
        names.append(p.name)
        if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD):
            positional += 1
            if p.default is p.empty:
                required_pos_names.append(p.name)
        elif p.kind is p.KEYWORD_ONLY:
            if p.default is p.empty:
                required_kwonly.add(p.name)
        elif p.kind is p.VAR_POSITIONAL:
            var_pos = True
        elif p.kind is p.VAR_KEYWORD:
            var_kw = True
    return _Params(
        tuple(names),
        positional,
        frozenset(required_kwonly),
        var_pos,
        var_kw,
        tuple(required_pos_names),
    )


def _params_from_ast(node: ast.FunctionDef | ast.AsyncFunctionDef) -> _Params:
    a = node.args
    pos = [*a.posonlyargs, *a.args]
    pos = [p for p in pos if p.arg != "self"]
    names = [p.arg for p in pos]
    n_required = max(len(pos) - len(a.defaults), 0)
    required_kwonly = {p.arg for p, d in zip(a.kwonlyargs, a.kw_defaults, strict=True) if d is None}
    names += [p.arg for p in a.kwonlyargs]
    return _Params(
        tuple(names),
        len(pos),
        frozenset(required_kwonly),
        a.vararg is not None,
        a.kwarg is not None,
        tuple(names[:n_required]),
    )


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


def _local_defs(tree: ast.Module) -> dict[str, _Params | None]:
    """Function defs by name; None marks a name defined more than once with
    differing signatures, which this scan cannot resolve unambiguously."""
    seen: dict[str, _Params | None] = {}
    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            continue
        params = _params_from_ast(node)
        if node.name in seen and seen[node.name] != params:
            seen[node.name] = None
        else:
            seen[node.name] = params
    return seen


def _dotted(node: ast.expr, aliases: dict[str, str]) -> str | None:
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


def _resolve(target: str) -> object | None:
    """Import *target*'s module and return the attribute it names."""
    module_path, _, attr = target.rpartition(".")
    while module_path:
        try:
            mod = importlib.import_module(module_path)
        except Exception:  # Why: an unimportable target is simply unresolvable here.
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


@dataclass(frozen=True, slots=True)
class _Finding:
    file: str
    lineno: int
    target: str
    double: str
    real: _Params
    stub: _Params

    def describe(self) -> str:
        return (
            f"  {self.file}:{self.lineno}  {self.double}() replaces {self.target}\n"
            f"      real  : ({', '.join(self.real.names)})\n"
            f"      double: ({', '.join(self.stub.names)})"
        )


def _scan() -> tuple[list[_Finding], list[_Finding], int]:
    """Return (hard drift, soft drift, resolved count) across the test tree."""
    hard: list[_Finding] = []
    soft: list[_Finding] = []
    resolved = 0
    for path in sorted(_TESTS_DIR.rglob("*.py")):
        if path == Path(__file__).resolve():
            continue
        try:
            tree = ast.parse(path.read_text())
        except SyntaxError:  # Why: a file that cannot parse is the parser's problem, not ours.
            continue
        aliases = _import_aliases(tree)
        defs = _local_defs(tree)
        for node in ast.walk(tree):
            if not (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "setattr"
                and isinstance(node.func.value, ast.Name)
                and node.func.value.id == "monkeypatch"
            ):
                continue
            target, replacement = _setattr_target_and_replacement(node, aliases)
            if target is None or replacement is None:
                continue
            stub = defs.get(replacement)
            if stub is None:
                continue
            real_obj = _resolve(target)
            if real_obj is None or not callable(real_obj):
                continue
            real = _params_from_signature(real_obj)
            if real is None:
                continue
            resolved += 1
            rel = str(path.relative_to(_TESTS_DIR))
            finding = _Finding(rel, node.lineno, target, replacement, real, stub)
            if _unsatisfiable(real, stub):
                hard.append(finding)
            elif _narrower(real, stub):
                soft.append(finding)
    return hard, soft, resolved


def _unsatisfiable(real: _Params, stub: _Params) -> bool:
    """Whether a caller written against *real* must fail to call *stub*.

    Such a caller fills the stub's leading parameters positionally, up to the
    number of positional slots the real signature offers. Anything the stub
    still requires past that point has to arrive by keyword, which only works
    if the real signature carries that name.

    This distinction is what keeps the check honest rather than noisy: a
    double ``(dsn, timeout)`` standing in for ``connect(dsn, *, timeout=...)``
    is fine, because ``timeout`` is passed by keyword and the double accepts
    it as one. A double ``(semaphore, pool, schema, topic)`` standing in for
    ``(semaphore, pool, schema)`` is not: nothing can ever supply ``topic``.
    """
    slots = stub.positional if real.accepts_var_positional else real.positional
    unfillable = stub.required_names_beyond(slots) - set(real.names)
    return bool(unfillable) or bool(stub.required_kwonly - set(real.names))


def _narrower(real: _Params, stub: _Params) -> bool:
    """Whether the real signature permits a call the stub could not accept.

    Parameter RENAMES are deliberately not reported: at equal arity a
    positional call still works, so a rename is a readability problem for
    review to catch, not a signature the caller cannot satisfy. Reporting them
    would bury the arity findings that actually break.
    """
    if stub.accepts_var_positional and stub.accepts_var_keyword:
        return False
    if real.positional > stub.positional and not stub.accepts_var_positional:
        return True
    # Keyword-only parameters the stub has no name for and no **kwargs to
    # swallow. Positional names are compared by COUNT above, not by name, so
    # a rename at equal arity does not land here.
    keyword_only_names = set(real.names[real.positional :])
    return bool(keyword_only_names - set(stub.names)) and not stub.accepts_var_keyword


def _setattr_target_and_replacement(
    node: ast.Call, aliases: dict[str, str]
) -> tuple[str | None, str | None]:
    """Both monkeypatch.setattr forms: (obj, "attr", repl) and ("dotted.attr", repl)."""
    if len(node.args) == 3:
        owner, attr, repl = node.args
        if not (isinstance(attr, ast.Constant) and isinstance(attr.value, str)):
            return None, None
        base = _dotted(owner, aliases)
        target = f"{base}.{attr.value}" if base else None
    elif len(node.args) == 2:
        dotted, repl = node.args
        if not (isinstance(dotted, ast.Constant) and isinstance(dotted.value, str)):
            return None, None
        target = dotted.value
    else:
        return None, None
    return target, repl.id if isinstance(repl, ast.Name) else None


def test_the_scan_resolves_enough_doubles_to_be_meaningful() -> None:
    """A guard on the guard: an import rename or an AST change could silently
    reduce this scan to zero doubles, and a scan of nothing always passes."""
    _hard, _soft, resolved = _scan()
    assert resolved >= 30, (
        f"only {resolved} monkeypatched doubles resolved to a real symbol; the "
        "scan below is no longer covering the suite (an import-alias or AST "
        "regression in this file, most likely)"
    )


def test_no_double_declares_a_signature_the_real_callable_cannot_satisfy() -> None:
    """Hard drift: the double demands arguments the real caller cannot supply,
    so every call through it raises TypeError."""
    hard, _soft, _resolved = _scan()
    assert not hard, (
        "Test doubles whose signature the real callable cannot satisfy — every "
        "call through these raises TypeError:\n\n"
        + "\n".join(f.describe() for f in hard)
        + "\n\nUpdate the double to mirror the real signature."
    )


def test_narrower_doubles_are_declared_deliberately() -> None:
    """Soft drift: a double narrower than the real callable works only while
    production does not pass the missing argument. Allowed only on the explicit
    list above, so a reader can tell a considered exception from an oversight."""
    _hard, soft, _resolved = _scan()
    undeclared = [f for f in soft if (f.file, f.target) not in _NARROWER_BY_DESIGN]
    assert not undeclared, (
        "Test doubles narrower than the callable they replace, and not on the "
        "_NARROWER_BY_DESIGN list:\n\n"
        + "\n".join(f.describe() for f in undeclared)
        + "\n\nEither widen the double to mirror the real signature, or add it to "
        "_NARROWER_BY_DESIGN with a Why: explaining that the unmodelled "
        "parameters are never passed."
    )


def test_the_allowlist_has_no_stale_entries() -> None:
    """An allowlisted double that no longer drifts means the entry outlived its
    reason and should go, before it starts excusing a future regression."""
    _hard, soft, _resolved = _scan()
    live = {(f.file, f.target) for f in soft}
    stale = sorted(key for key in _NARROWER_BY_DESIGN if key not in live)
    assert not stale, (
        "_NARROWER_BY_DESIGN entries that no longer correspond to a narrower "
        "double:\n" + "\n".join(f"  - {f} -> {t}" for f, t in stale) + "\nRemove them."
    )

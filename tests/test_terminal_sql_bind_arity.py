"""Every mark_* statement's placeholder count equals every caller's bind count.

The defect class: a statement gains a placeholder (mark_snoozed's
denial_reason rode in as ``$9``) while one of its callers keeps passing the
old bind list. asyncpg rejects the call at runtime with ``InterfaceError:
the server expects N arguments for this query, M were passed``, and the
in-memory coverage never sees it: a mock or a twin answers whatever the
caller supplies, and parameter arity is exactly what a mock cannot supply.

Mechanism: render every ``SqlTemplates`` mark_* statement through the
bundle's own ``render()``, count its distinct ``$N`` placeholders, and
assert the numbering is dense (``$1`` through the maximum, no gaps, so a
placeholder renumbered without its siblings fails here before it fails in
CI). Then walk the source of every module that fetches a mark_* statement
with ``ast``, resolve each ``fetch*``/``execute`` first argument to the
statement (or statements, through a conditional or an alias) it can carry,
and assert the call's positional bind count equals the statement's
placeholder count.

Scope, stated directly:

* Every module under ``src/taskq`` is walked, not a hand-listed pair: a
  mark_* statement executed through a helper or a new call site joins the
  net on arrival instead of hiding in a module the pin never named. The
  Postgres terminal module supplies the nine production call paths today
  (the completeness tripwire fails if the net loses or gains one); the
  in-memory twin ships no SQL today, its writes are the Python mirrors the
  differential corpus pins, so the same net walks it too and arity-checks
  any statement that ever appears there instead of the module being
  exempted silently.
* The test suite's own direct-SQL callers are walked like production code:
  the drifted call that motivated this pin was a test rendering
  ``sql.mark_snoozed`` by hand inside a transaction, passing the
  pre-denial-reason eight-bind list against the nine-placeholder statement.

Deliberately out of scope: the non-terminal statements (each is assembled
and bound in one place, and the parse-and-plan smoke guard validates their
text) and keyword binds (every mark_* call binds positionally, asserted
below, so the positional count is the whole bind list).
"""

from __future__ import annotations

import ast
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Final

import pytest

from taskq.backend._sql_templates import SqlTemplates, render

_REPO_ROOT: Final = Path(__file__).resolve().parent.parent

# Both backends' terminal-write modules. The net does not assume which one
# ships SQL: it arity-checks whatever it finds and the completeness tripwire
# pins the Postgres side's nine statements.
_TERMINAL_MODULES: Final[tuple[Path, ...]] = (
    _REPO_ROOT / "src" / "taskq" / "backend" / "_terminal.py",
    _REPO_ROOT / "src" / "taskq" / "testing" / "_terminal.py",
)
_SRC_DIR: Final = _REPO_ROOT / "src" / "taskq"
_TESTS_DIR: Final = _REPO_ROOT / "tests"

# The nine terminal-write statements the bundle ships, the whole mark_*
# surface of SqlTemplates.
_STATEMENT_ATTRS: Final[tuple[str, ...]] = tuple(
    field.name
    for field in SqlTemplates.__dataclass_fields__.values()  # pyright: ignore[reportAttributeAccessIssue]  # Why: the pin must follow the dataclass's own field list so a new terminal statement joins the net on arrival.
    if field.name.startswith("mark_")
)

_PLACEHOLDER_RE: Final = re.compile(r"\$(\d+)")

# The asyncpg query methods a mark_* statement can reach a server through.
_FETCH_METHODS: Final = frozenset({"fetch", "fetchrow", "fetchval", "execute"})


@dataclass(frozen=True, slots=True)
class _CallSite:
    """One discovered call site: where, which statements, how many binds."""

    module: str
    line: int
    statements: frozenset[str]
    bind_count: int


def _rendered_statements() -> dict[str, str]:
    """Render the production bundle for a throwaway schema and keep the
    mark_* statements only. Rendering through ``render()`` is the point:
    the pin counts the exact text production sends."""
    templates = render("bind_arity_pin")
    return {name: getattr(templates, name) for name in _STATEMENT_ATTRS}


def _statement_attrs(
    node: ast.expr,
    aliases: dict[str, ast.expr],
    _seen: frozenset[str] = frozenset(),
) -> frozenset[str] | None:
    """Resolve a call's first argument to the mark_* statement(s) it can
    carry: a direct ``sql.mark_x`` attribute, a conditional between two,
    or an assignment alias of either. ``None`` when the argument is not a
    mark_* statement reference. ``_seen`` breaks alias cycles (a
    self-referential default like ``x = x if x is None else x``).
    """
    if isinstance(node, ast.Attribute):
        if node.attr.startswith("mark_"):
            return frozenset({node.attr})
        return None
    if isinstance(node, ast.IfExp):
        body = _statement_attrs(node.body, aliases, _seen)
        orelse = _statement_attrs(node.orelse, aliases, _seen)
        if body is None or orelse is None:
            return None
        return body | orelse
    if isinstance(node, ast.Name) and node.id in aliases and node.id not in _seen:
        return _statement_attrs(aliases[node.id], aliases, _seen | {node.id})
    return None


def _aliases(tree: ast.Module) -> tuple[dict[str, ast.expr], frozenset[str]]:
    """Simple ``name = <expr>`` bindings anywhere in the module (the test
    files bind ``stmt = sql.mark_x`` style intermediates). Last wins.

    Also returns the names assigned MORE THAN ONCE where any assignment is
    a mark_* statement reference: a conditional re-bind (``if``/``else``
    arms assigning the same name) can carry more statements than the one
    the last-wins map resolves it to, so a fetch through such a name is
    ambiguous and the caller must fail loudly rather than silently check
    only the surviving arm. A single assignment between two statements
    (``stmt = a if flag else b``) is not ambiguous: the resolver expands
    the conditional expression itself.
    """
    found: dict[str, ast.expr] = {}
    assigned: dict[str, list[ast.expr]] = {}
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Assign)
            and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Name)
        ):
            assigned.setdefault(node.targets[0].id, []).append(node.value)
            found[node.targets[0].id] = node.value
    ambiguous = frozenset(
        name
        for name, values in assigned.items()
        if len(values) > 1 and any(_statement_attrs(value, found) is not None for value in values)
    )
    return found, ambiguous


def _call_sites(tree: ast.Module, module: str) -> list[_CallSite]:
    """Every fetch-style call whose statement argument resolves to one or
    more mark_* statements, with its positional bind count."""
    aliases, ambiguous = _aliases(tree)
    sites: list[_CallSite] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if not (isinstance(func, ast.Attribute) and func.attr in _FETCH_METHODS):
            continue
        if not node.args:
            continue
        # An ambiguous alias (conditionally reassigned, any arm a mark_*
        # reference) fails loudly even when the last-wins map resolves it:
        # the surviving arm is only one of the statements the name can
        # carry at runtime, and the others' arity would go unchecked.
        assert not (isinstance(node.args[0], ast.Name) and node.args[0].id in ambiguous), (
            f"{module}:{node.lineno}: a fetch through a conditionally "
            "reassigned statement alias cannot be resolved to one "
            "statement set; give the call its own explicit pin entry or "
            "resolve the arms directly"
        )
        statements = _statement_attrs(node.args[0], aliases)
        if statements is None:
            continue
        assert not node.keywords, (
            f"{module}:{node.lineno}: a mark_* call binds positionally; a "
            "keyword bind makes the positional count meaningless and needs "
            "its own arity pin entry"
        )
        sites.append(
            _CallSite(
                module=module,
                line=node.lineno,
                statements=frozenset(statements),
                # First positional argument is the statement itself.
                bind_count=len(node.args) - 1,
            )
        )
    return sites


def _tree(path: Path) -> ast.Module:
    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def _assert_arity(sites: list[_CallSite], rendered: dict[str, str]) -> None:
    for site in sites:
        for name in sorted(site.statements):
            # Distinct numbers, not occurrences: a statement may reference
            # the same placeholder in several CTEs, asyncpg still counts
            # one argument per distinct $N.
            placeholders = sorted({int(n) for n in _PLACEHOLDER_RE.findall(rendered[name])})
            assert site.bind_count == len(placeholders), (
                f"{site.module}:{site.line} binds {site.bind_count} argument(s) "
                f"for {name}, which carries {len(placeholders)} placeholder(s) "
                f"({placeholders}); every caller's bind count must equal the "
                "statement's placeholder count"
            )


@pytest.fixture(name="rendered", scope="module")
def _rendered() -> dict[str, str]:
    return _rendered_statements()


def test_pin_covers_the_whole_mark_surface(rendered: dict[str, str]) -> None:
    """The net's statement inventory is the dataclass's own mark_* field
    list, so the pin cannot silently cover a subset."""
    assert set(rendered) == set(_STATEMENT_ATTRS)
    assert len(_STATEMENT_ATTRS) == 9


def test_placeholder_numbering_is_dense(rendered: dict[str, str]) -> None:
    """Every statement numbers its placeholders ``$1`` through the maximum
    with no gaps: asyncpg binds positionally, a gap means a placeholder was
    renumbered or dropped without its siblings."""
    for name, statement in rendered.items():
        numbers = {int(n) for n in _PLACEHOLDER_RE.findall(statement)}
        assert numbers == set(range(1, max(numbers) + 1)), (
            f"{name} carries non-dense placeholder numbering {sorted(numbers)}"
        )


def test_backend_call_sites_match_placeholder_arity(rendered: dict[str, str]) -> None:
    """Every mark_* fetch anywhere in the package binds exactly the
    statement's placeholder count, and the Postgres side's net still sees
    all nine statements (a walk that lost a call site would pass
    vacuously otherwise)."""
    sites = [
        site
        for path in sorted(_SRC_DIR.rglob("*.py"))
        if "__pycache__" not in path.parts
        for site in _call_sites(_tree(path), module=str(path.relative_to(_REPO_ROOT)))
    ]
    pg_module = str(_TERMINAL_MODULES[0].relative_to(_REPO_ROOT))
    twin_module = str(_TERMINAL_MODULES[1].relative_to(_REPO_ROOT))
    pg_statements = {s for site in sites if site.module == pg_module for s in site.statements}
    twin_statements = {s for site in sites if site.module == twin_module for s in site.statements}
    assert pg_statements == set(_STATEMENT_ATTRS), (
        f"the arity net lost Postgres call sites; found {sorted(pg_statements)}"
    )
    # The twin ships no SQL today (its writes are the Python mirrors the
    # differential corpus pins); this walk keeps its surface covered the
    # day a statement appears there.
    assert not twin_statements, (
        f"{twin_module} grew SQL call sites for {sorted(twin_statements)}; "
        "they are arity-checked below like any production caller"
    )
    _assert_arity(sites, rendered)


def test_direct_sql_test_callers_match_placeholder_arity(rendered: dict[str, str]) -> None:
    """The suite's own direct-SQL callers (tests rendering a template by
    hand against a live connection) bind the statement's full placeholder
    count too: the drifted caller this pin exists for was one of these."""
    sites = [
        site
        for path in sorted(_TESTS_DIR.rglob("*.py"))
        if path.name != Path(__file__).name
        for site in _call_sites(_tree(path), module=str(path.relative_to(_TESTS_DIR)))
    ]
    seen: set[str] = set()
    for site in sites:
        seen |= site.statements
    # Rot guard: the suite renders at least the snooze and failed
    # statements by hand today; if the net starts finding none, it broke.
    assert "mark_snoozed" in seen and "mark_failed" in seen, (
        f"the arity net lost the direct-SQL test callers; found {sorted(seen)}"
    )
    _assert_arity(sites, rendered)

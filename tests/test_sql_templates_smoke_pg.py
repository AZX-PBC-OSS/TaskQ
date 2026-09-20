"""Parse-and-plan smoke guard over every SQL statement the package ships.

The defect class: a hard PostgreSQL syntax error committed into a shipped
statement. The stranded-jobs detector's query went out with a duplicated
``SELECT r.actor,`` line in its inner select - a ``PostgresSyntaxError``
on every tick against a real database - while the loop's mocked-connection
coverage passed in full and could never see it: a mock answers whatever
the test supplies, and parse validity is exactly what a mock cannot
supply. A guard that cannot catch that class is theatre; this file exists
so the whole class fails at authoring time.

Mechanism: ``asyncpg.Connection.prepare`` against a real, fully migrated
schema (the suite's ``module_pg_schema`` fixture). Prepare parses AND
plans against the catalog with ``$n`` parameter types inferred and
executes nothing - the parse-and-validate semantics an ``EXPLAIN``
wrapper would give, without fabricating parameter values and without
running writes. A duplicated line, an unbalanced CTE, or a reference to a
renamed column all fail here, naming the statement.

Scope, stated directly:

* Every ``SqlTemplates`` dataclass field, rendered per schema through the
  module's own ``render()`` (``taskq.backend._sql_templates``). The two
  column-list tuple fields are not statements and drop out by type.
* Every module-level string constant under ``taskq.backend``,
  ``taskq.worker`` and ``taskq.ratelimit`` whose text carries a SQL
  statement keyword - walked, not listed, so a new statement fails here
  on arrival (the ``tests/test_sweepaudit_bounded_writes.py`` doctrine).
  Constants that are not self-contained statements are never skipped
  silently: interpolation fragments and the ``__TOKEN__`` dispatch
  template are registered in ``_COVERED_BY`` with the rendered product
  whose prepare validates them (interpolation is verbatim, so the
  product's parse covers the fragment's syntax, and a containment
  tripwire keeps the registration honest), the one prefix constant is
  prepared through the completion shape its sole call site appends, and
  the non-PostgreSQL strings (the Redis Lua sources, an operator-facing
  validation message) are registered in ``_NOT_PG_SQL`` with a marker
  tripwire.
* ``worker/leader.py``'s lease constants are module-level and walked like
  everything else. The stranded-jobs detector's query in
  ``worker/_leader_sweeps.py`` - the site the duplicated line shipped
  in - is function-local, so it is extracted from the module's own AST
  (the exact source text the loop renders, never a restated copy) and
  prepared like every other statement.

Deliberately out of scope: ``taskq.migrations`` (applied for real by the
migration suites and the schema fixtures), function-local SQL other than
the detector query (assembled at call sites from walked constants and
parameters - the bounded-writes audit's scope statement covers that
discipline), and top-level modules such as ``taskq.actor_config_ops`` -
the walk's roots are the three shipped subpackages above; widening them
is a one-line change when that surface wants the same guard.
"""

from __future__ import annotations

import ast
import importlib
import inspect
import pkgutil
import re
from collections.abc import Callable
from dataclasses import fields
from typing import Final

import asyncpg
import pytest

import taskq.backend
import taskq.ratelimit
import taskq.worker
import taskq.worker._leader_sweeps as leader_sweeps_mod  # pyright: ignore[reportPrivateUsage]  # Why: the detector query is function-local in this module; the guard extracts the exact source statement (module docstring) rather than restating a copy that could drift.
from taskq.backend._batch_sql import (  # pyright: ignore[reportPrivateUsage]  # Why: the batch statements' {open_member} probe is rendered by this module's own helper and bundle; the guard renders through them so its text is the production text, never a copy.
    BatchSql,
    render_batch_sql,
)
from taskq.backend._sql_templates import (  # pyright: ignore[reportPrivateUsage]  # Why: rendering the production bundle through its own render() is the point of the guard; a re-derived copy would drift from the SQL that actually runs.
    SqlTemplates,
    render,
)
from taskq.constants import RECLAIM_OUTBOX_RETENTION_MULTIPLIER
from taskq.testing.fixtures import ModulePgSchema
from taskq.worker._leader_shared import complete_stale_batches_sql

pytestmark = pytest.mark.integration

_WALK_ROOTS: Final = (taskq.backend, taskq.worker, taskq.ratelimit)

# The discovery net: any module-level string carrying a statement keyword.
# The completeness test below then forces EVERY discovered constant to be
# either prepared or explicitly registered - nothing drops out silently.
# A constant the net cannot see (a pure expression fragment with no
# statement keyword, e.g. the reclaim budget predicate) is only ever
# interpolated into a discovered statement and is validated through that
# product's prepare.
_SQL_KEYWORD_RE: Final = re.compile(
    r"\b(SELECT|INSERT|UPDATE|DELETE|WITH|VALUES|CREATE|ALTER|DROP|LISTEN|NOTIFY|COPY)\b",
    re.IGNORECASE,
)
_PLACEHOLDER_RE: Final = re.compile(r"\{(\w+)\}")
_STATEMENT_KEYWORDS: Final = frozenset({"SELECT", "INSERT", "UPDATE", "DELETE", "WITH", "VALUES"})
_DISPATCH_TOKEN_RE: Final = re.compile(r"__[A-Z_]+__")


def _leading_keyword(sql: str) -> str | None:
    """First meaningful word of *sql*, upper-cased, after stripping leading
    whitespace and ``--``/``/* */`` comments (several shipped statements open
    with a rationale banner). ``None`` when nothing but trivia remains."""
    text = sql
    while True:
        text = text.lstrip()
        if text.startswith("--"):
            text = text.partition("\n")[2]
        elif text.startswith("/*"):
            text = text.partition("*/")[2]
        else:
            break
    match = re.match(r"[A-Za-z]+", text)
    return match.group(0).upper() if match else None


def _discover_sql_constants() -> dict[str, str]:
    """Walk every module under the three in-scope packages and return
    {qualified_name: body} for module-level string constants carrying a SQL
    statement keyword.

    Re-exports resolve to the same str object and are deduplicated by
    identity. Modules whose optional dependencies are missing are skipped
    rather than failing the walk - the known-guarded shapes (the extras'
    documented ImportErrors) only; anything else re-raises. Same walk
    discipline as ``tests/test_sweepaudit_bounded_writes.py``.
    """
    found: dict[str, str] = {}
    seen_ids: set[int] = set()
    modules = list(_WALK_ROOTS)
    for root in _WALK_ROOTS:
        for info in pkgutil.walk_packages(root.__path__, prefix=f"{root.__name__}."):
            try:
                modules.append(importlib.import_module(info.name))
            except ImportError as exc:
                known_extras = ("taskq[",)
                if (
                    exc.args
                    and isinstance(exc.args[0], str)
                    and exc.args[0].startswith(known_extras)
                ):
                    continue
                raise
    for mod in modules:
        for name, val in inspect.getmembers(mod, lambda v: isinstance(v, str)):
            if name.startswith("__") or not _SQL_KEYWORD_RE.search(val) or id(val) in seen_ids:
                continue
            seen_ids.add(id(val))
            found[f"{mod.__name__}:{name}"] = val
    return found


# ── Render extras: placeholders production fills from sibling constants ──
#
# Every placeholder other than ``schema`` must resolve here through the
# owning module's own render inputs. A new placeholder that arrives without
# a registered source fails the render loudly - the failure says where to
# register it.


_POSITIONAL_PARAM_RE: Final = re.compile(r"\$(\d+)")


def _render_extra(placeholder: str, owning_module: str, body: str) -> object:
    if placeholder == "open_member":
        # taskq.backend._batch_sql renders {open_member} through its own
        # open_member_where("$N"), binding the batch id as the next
        # positional parameter after the ones the statement already
        # carries ($2 for the counter and completion writes, $1 for the bare
        # count). The rendered text is pinned to the production bundle by
        # the gaps test's _batch_open_member_render_matches_bundle tripwire.
        helper = getattr(importlib.import_module(owning_module), "open_member_where", None)
        if not callable(helper):
            raise AssertionError(
                f"{owning_module}.open_member_where is gone; the open-member "
                "probe's render path changed — re-review this resolver"
            )
        next_param = max((int(n) for n in _POSITIONAL_PARAM_RE.findall(body)), default=0) + 1
        return helper(f"${next_param}")
    if placeholder == "terminal_not_in":
        # Each module carrying {terminal_not_in} templates defines its own
        # _TERMINAL_NOT_IN over TERMINAL_STATUSES; rendering with the owning
        # module's constant is that module's own render path.
        value: object = getattr(importlib.import_module(owning_module), "_TERMINAL_NOT_IN", None)
        if not isinstance(value, str):
            raise AssertionError(
                f"{owning_module}._TERMINAL_NOT_IN is no longer a string; "
                "the terminal-status fragment's shape changed — re-review this resolver"
            )
        return value
    if placeholder == "outbox_multiplier":
        return RECLAIM_OUTBOX_RETENTION_MULTIPLIER
    raise AssertionError(
        f"unregistered render placeholder {{{placeholder}}} on a constant in "
        f"{owning_module} - register the value production substitutes in "
        "_render_extra (from the module's own render path, never a copy)"
    )


# Constants whose owning module ships its own renderer: the guard prepares
# exactly what production executes by calling it, so no per-placeholder
# resolution can drift from the module's render path. The completeness
# test fails a stale entry (the constant renamed or gone).
_OWN_RENDERERS: Final[dict[str, Callable[[str], str]]] = {
    "taskq.worker._leader_shared:_COMPLETE_STALE_BATCHES_SQL": complete_stale_batches_sql,
}


def _render_constant(qualified: str, body: str, schema: str) -> str:
    """Render one discovered constant the way its owning module does: the
    module's own renderer when it ships one, else ``.format(schema=...)``
    plus the registered extras for any further placeholders."""
    own_renderer = _OWN_RENDERERS.get(qualified)
    if own_renderer is not None:
        return own_renderer(schema)
    placeholders = set(_PLACEHOLDER_RE.findall(body)) - {"schema"}
    if not placeholders:
        return body.format(schema=schema)
    owning_module = qualified.rsplit(":", 1)[0]
    extras = {ph: _render_extra(ph, owning_module, body) for ph in placeholders}
    return body.format(schema=schema, **extras)


# ── Registries: the discovered constants this guard does NOT prepare raw ──
#
# Every entry is a claim with a tripwire, same doctrine as the bounded-writes
# audit's _EXEMPT: the reason is the review, and the completeness test below
# fails a stale entry (the constant renamed, the wiring changed, the marker
# gone) as loudly as an unregistered arrival.

# Interpolation fragments and token templates, validated through the rendered
# product(s) that carry them: qualified name -> (products, marker, reason).
# A None marker pins VERBATIM containment - the product's text must contain
# the fragment's whole current body, which is how str.replace interpolation
# works; a str marker is a distinctive line that must survive the product's
# token substitution.
_COVERED_BY: Final[dict[str, tuple[tuple[str, ...], str | None, str]]] = {
    "taskq.backend._dispatch_sql:_DISPATCH_SQL_TEMPLATE": (
        (
            "taskq.backend._dispatch_sql:DISPATCH_STRICT_FIFO_SQL",
            "taskq.backend._dispatch_sql:DISPATCH_ROUND_ROBIN_SQL",
        ),
        "-- WITH RECURSIVE: both variants' label-routed keys enumerations",
        "the shared dispatch template still carries __TOKEN__ fragment holes; "
        "its two rendered variants are the statements production sends",
    ),
    "taskq.backend._dispatch_sql:_RR_KEYS_CTE": (
        ("taskq.backend._dispatch_sql:DISPATCH_ROUND_ROBIN_SQL",),
        None,
        "a CTE fragment, never a statement; interpolated verbatim into the "
        "round-robin dispatch variant",
    ),
    "taskq.backend._dispatch_sql:_PA_KEYS_CTE": (
        ("taskq.backend._dispatch_sql:DISPATCH_STRICT_FIFO_SQL",),
        None,
        "a CTE fragment, never a statement; interpolated verbatim into the "
        "strict-FIFO dispatch variant",
    ),
    "taskq.backend._dispatch_sql:_STRICT_FIFO_CANDIDATES_LATERAL": (
        ("taskq.backend._dispatch_sql:DISPATCH_STRICT_FIFO_SQL",),
        None,
        "a correlated lateral fragment, never a standalone statement; "
        "interpolated verbatim into the strict-FIFO dispatch variant",
    ),
    "taskq.backend._dispatch_sql:_ROUND_ROBIN_CANDIDATES_LATERAL": (
        ("taskq.backend._dispatch_sql:DISPATCH_ROUND_ROBIN_SQL",),
        None,
        "a correlated lateral fragment, never a standalone statement; "
        "interpolated verbatim into the round-robin dispatch variant",
    ),
    "taskq.backend._dispatch_sql:_REPENDED_STRICT_FIFO_LATERAL": (
        ("taskq.backend._dispatch_sql:DISPATCH_STRICT_FIFO_SQL",),
        None,
        "a correlated lateral fragment, never a standalone statement; "
        "interpolated verbatim into the strict-FIFO dispatch variant",
    ),
    "taskq.backend._dispatch_sql:_REPENDED_ROUND_ROBIN_LATERAL": (
        ("taskq.backend._dispatch_sql:DISPATCH_ROUND_ROBIN_SQL",),
        None,
        "a correlated lateral fragment, never a standalone statement; "
        "interpolated verbatim into the round-robin dispatch variant",
    ),
    "taskq.backend._sweeps:_SWEEP_1_BODY": (
        ("taskq.backend._sweeps:_SWEEP_1_SQL",),
        "-- Leader-only reclaim sweep",
        "an intermediate carrying the {has_budget}/{reclaim_delay} and "
        "{worker_crashed_class} holes; _SWEEP_1_SQL is the rendered statement "
        "production sends",
    ),
    "taskq.backend._sweeps:_SWEEP_IDLE_KEYED_BUCKETS_BODY": (
        ("taskq.backend._sweeps:_SWEEP_IDLE_KEYED_BUCKETS_SQL",),
        "-- The consumed-fixed-quota veto is this table's analogue of the",
        "an intermediate carrying the quota-predicate holes; "
        "_SWEEP_IDLE_KEYED_BUCKETS_SQL is the rendered statement production sends",
    ),
    "taskq.backend._sql_fragments:_JOB_FENCE_SQL": (
        (
            "taskq.backend._sql_templates:SqlTemplates.mark_retry",
            "taskq.backend._sql_templates:SqlTemplates.mark_snoozed",
            "taskq.backend._sql_templates:SqlTemplates.mark_retry_after_consume_true",
            "taskq.backend._sql_templates:SqlTemplates.mark_retry_after_consume_false",
            "taskq.backend._sql_templates:SqlTemplates.mark_interrupted",
        ),
        None,
        "a fence-conjunct fragment, never a standalone statement; interpolated "
        "verbatim into the five multi-arm terminal arbiters, so their rendered "
        "bundle fields (resolved through the prepared inventory, the arbiters "
        "are built inside render()) carry and validate its text",
    ),
}

# Prefix constants completed at their sole call site: qualified name ->
# (completion suffix, reason). The suffix is the exact shape the call site
# appends; the tripwire asserts the constant stays a prefix.
_PREFIX_COMPLETIONS: Final[dict[str, tuple[str, str]]] = {
    "taskq.backend._schedules:_SCHEDULE_UPDATE_SQL": (
        " enabled = $2 WHERE id = $1 RETURNING *",
        "prefix constant; update_schedule completes it with "
        "'<col> = $n ... WHERE id = $1 RETURNING *' — validated through that "
        "completion shape",
    ),
}

# Strings the net catches that are not PostgreSQL at all: qualified name ->
# (marker that must survive in the body, reason).
_NOT_PG_SQL: Final[dict[str, tuple[str, str]]] = {
    "taskq.backend._protocol:_QUEUE_NAME_RULE": (
        "queue name must start with",
        "an operator-facing validation message; no statement is sent",
    ),
    "taskq.ratelimit._scripts:_LUA_SRC": (
        "redis.call",
        "Lua source for Redis EVALSHA; PostgreSQL parse validation does not apply",
    ),
    "taskq.ratelimit._scripts:_REFUND_SRC": (
        "redis.call",
        "Lua source for Redis EVALSHA; PostgreSQL parse validation does not apply",
    ),
    "taskq.ratelimit._scripts:_SLIDING_WINDOW_GCRA_SRC": (
        "redis.call",
        "Lua source for Redis EVALSHA; PostgreSQL parse validation does not apply",
    ),
}


def _extract_stranded_detector_sql() -> str:
    """The exact ``_stranded_sql`` literal ``_stranded_jobs_loop`` renders.

    The detector's query is function-local, so the module walk cannot see
    it - and it is the site the committed syntax error shipped in, so the
    guard must see it. AST extraction takes the statement from the module's
    own source, never a restated copy; a refactor that renames the local,
    builds it dynamically, or splits it into more than one assignment fails
    the exactly-one tripwire loudly instead of dropping the site's coverage.
    """
    tree = ast.parse(inspect.getsource(leader_sweeps_mod))
    found: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            continue
        if node.name != "_stranded_jobs_loop":
            continue
        for sub in ast.walk(node):
            if not isinstance(sub, ast.Assign):
                continue
            if not any(isinstance(t, ast.Name) and t.id == "_stranded_sql" for t in sub.targets):
                continue
            try:
                value: object = ast.literal_eval(sub.value)
            except (ValueError, TypeError, SyntaxError) as exc:
                raise AssertionError(
                    "_stranded_sql in _stranded_jobs_loop is no longer a plain "
                    "string literal; the extraction above must follow the new "
                    "shape before the guard can vouch for the site"
                ) from exc
            if not isinstance(value, str):
                raise AssertionError("_stranded_sql in _stranded_jobs_loop is no longer a string")
            found.append(value)
    if len(found) != 1:
        raise AssertionError(
            f"expected exactly one _stranded_sql assignment in "
            f"_stranded_jobs_loop, found {len(found)} - the detector's query "
            "site moved; re-point this extraction so the guard keeps watching it"
        )
    return found[0]


_STRANDED_SOURCE: Final = "taskq.worker._leader_sweeps._stranded_jobs_loop:_stranded_sql"


def _build_inventory(schema: str) -> dict[str, list[str]]:
    """{rendered statement: [qualified source names]} for every statement the
    guard prepares. Rendered-text dedup: a module constant and the
    SqlTemplates field rendered from it are one statement with two names."""
    inventory: dict[str, list[str]] = {}

    def add(source: str, sql: str) -> None:
        inventory.setdefault(sql, []).append(source)

    for qualified, body in sorted(_discover_sql_constants().items()):
        if qualified in _NOT_PG_SQL or qualified in _COVERED_BY:
            continue
        if qualified in _PREFIX_COMPLETIONS:
            suffix = _PREFIX_COMPLETIONS[qualified][0]
            add(qualified, _render_constant(qualified, body, schema) + suffix)
            continue
        if _leading_keyword(body) in _STATEMENT_KEYWORDS:
            add(qualified, _render_constant(qualified, body, schema))
            continue
        raise AssertionError(
            f"{qualified} carries SQL keywords but is not a self-contained "
            "statement and is not registered — register it in _COVERED_BY "
            "(with the rendered product that validates it), "
            "_PREFIX_COMPLETIONS (with its call-site completion), or "
            "_NOT_PG_SQL (with the reason PostgreSQL validation does not apply)"
        )

    bundle = render(schema)
    for field in fields(SqlTemplates):
        value: object = getattr(bundle, field.name)
        if isinstance(value, str):
            add(f"taskq.backend._sql_templates:SqlTemplates.{field.name}", value)

    add(_STRANDED_SOURCE, _extract_stranded_detector_sql().format(schema=schema))
    return inventory


async def test_every_shipped_statement_parses_and_plans_against_a_live_schema(
    module_pg_schema: ModulePgSchema,
) -> None:
    """Every statement the package ships parses and plans against a real,
    fully migrated schema.

    ``conn.prepare`` infers ``$n`` parameter types from the statement's own
    casts and contexts, plans against the catalog, and executes nothing - a
    syntax error (the class the mocked-connection suites cannot see) fails
    here at authoring time, naming the statement and every source constant
    that renders to it.
    """
    inventory = _build_inventory(module_pg_schema.schema_name)
    conn = await asyncpg.connect(module_pg_schema.pg_dsn)
    failures: list[str] = []
    try:
        for sql, sources in sorted(inventory.items()):
            try:
                await conn.prepare(sql)
            except asyncpg.exceptions.PostgresError as exc:
                first_line = str(exc).splitlines()[0]
                failures.append(
                    f"  {type(exc).__name__}: {first_line}\n    rendered by: {', '.join(sources)}"
                )
    finally:
        await conn.close()
    assert not failures, (
        f"{len(failures)} shipped statement(s) failed parse/plan against a "
        "live, fully migrated schema - the defect class mocked-connection "
        "coverage cannot see:\n" + "\n".join(failures)
    )


def test_the_guard_has_no_silent_gaps() -> None:
    """The reverse direction, same doctrine as the bounded-writes audit's
    second test: every discovered constant is prepared or registered, every
    registration is still live, and every tripwire holds.

    A standard that nothing enforces erodes - this is what enforces the
    guard's coverage claim: a new statement that is not parseable raw fails
    here until it is registered with its reason, and a registration whose
    constant, product, or marker went away fails here until it is
    re-reviewed.
    """
    schema = "sql_smoke_guard"
    discovered = _discover_sql_constants()
    inventory = _build_inventory(schema)
    prepared_sources = {name for names in inventory.values() for name in names}
    # name -> rendered body for every source that reached the inventory,
    # rendered bundle fields included: a fragment's registered products may
    # name either a discovered constant or a rendered SqlTemplates field.
    rendered_by_name = {name: sql for sql, names in inventory.items() for name in names}

    unhandled = [
        name
        for name in discovered
        if name not in prepared_sources and name not in _COVERED_BY and name not in _NOT_PG_SQL
    ]
    assert not unhandled, (
        "Discovered constants neither prepared nor registered - the guard "
        "must account for every one:\n  " + "\n  ".join(unhandled)
    )

    stale = [
        name
        for name in (*_COVERED_BY, *_PREFIX_COMPLETIONS, *_NOT_PG_SQL)
        if name not in discovered
    ]
    assert not stale, (
        "Registry entries naming constants no longer discovered - the "
        "statement was renamed or removed; delete or re-point the entry:\n  " + "\n  ".join(stale)
    )

    broken_cover: list[str] = []
    for name, (products, marker, _reason) in _COVERED_BY.items():
        needle = discovered[name] if marker is None else marker
        for product in products:
            product_body = discovered.get(product) or rendered_by_name.get(product)
            if product_body is None:
                broken_cover.append(f"{name}: product {product} is no longer discovered")
                continue
            if product not in prepared_sources:
                broken_cover.append(
                    f"{name}: product {product} is not itself prepared - the "
                    "fragment's coverage is gone"
                )
            if needle not in product_body:
                broken_cover.append(
                    f"{name}: its marker no longer appears in {product} - the "
                    "interpolation wiring changed; re-review the registration"
                )
    assert not broken_cover, "Broken covered-by registrations:\n  " + "\n  ".join(broken_cover)

    # The dispatch template's tripwire is the reverse marker direction: the
    # rendered variants must carry NO fragment holes.
    token_leaks = [
        product
        for product in _COVERED_BY["taskq.backend._dispatch_sql:_DISPATCH_SQL_TEMPLATE"][0]
        if product in discovered and _DISPATCH_TOKEN_RE.search(discovered[product])
    ]
    assert not token_leaks, (
        "Rendered dispatch variants still carry __TOKEN__ holes - the "
        "template's fragments are no longer fully interpolated:\n  " + "\n  ".join(token_leaks)
    )

    # The open-member probe tripwire: every _batch_sql constant the guard
    # rendered with {open_member} must be, text for text, a field of the
    # bundle render_batch_sql ships - the guard's parameter numbering is
    # the module's own, not a drifting copy.
    bundle_statements = {
        getattr(render_batch_sql(schema), field.name) for field in fields(BatchSql)
    }
    open_member_drift = [
        name
        for name, body in discovered.items()
        if "{open_member}" in body
        and name not in _OWN_RENDERERS
        and _render_constant(name, body, schema) not in bundle_statements
    ]
    assert not open_member_drift, (
        "Open-member statements whose guard render is not a render_batch_sql "
        "field - the guard's {open_member} substitution drifted from "
        "_batch_sql's own render path:\n  " + "\n  ".join(open_member_drift)
    )
    assert any("{open_member}" in body for body in discovered.values()), (
        "No discovered constant carries {open_member} any more - the batch "
        "probe render changed shape; drop this tripwire and the resolver branch"
    )

    stale_renderers = [name for name in _OWN_RENDERERS if name not in discovered]
    assert not stale_renderers, (
        "_OWN_RENDERERS names constants the discovery no longer finds - the "
        "constant was renamed or removed; update the registry:\n  " + "\n  ".join(stale_renderers)
    )

    marker_drift = [
        name for name, (marker, _reason) in _NOT_PG_SQL.items() if marker not in discovered[name]
    ]
    assert not marker_drift, (
        "Non-PG registrations whose marker no longer matches - the string "
        "changed kind; re-review whether PostgreSQL validation now applies:\n  "
        + "\n  ".join(marker_drift)
    )

    no_longer_prefix = [
        name for name in _PREFIX_COMPLETIONS if not discovered[name].rstrip().endswith("SET")
    ]
    assert not no_longer_prefix, (
        "Prefix completions whose constant is no longer a bare SET prefix - "
        "the constant became a full statement; drop the completion and let "
        "it prepare directly:\n  " + "\n  ".join(no_longer_prefix)
    )

    # Every string-valued SqlTemplates field reached the inventory - a new
    # field the build loop cannot see would be a silent hole.
    bundle = render(schema)
    missing_fields = [
        field.name
        for field in fields(SqlTemplates)
        if isinstance(getattr(bundle, field.name), str)
        and f"taskq.backend._sql_templates:SqlTemplates.{field.name}" not in prepared_sources
    ]
    assert not missing_fields, (
        "SqlTemplates fields absent from the prepared inventory:\n  " + "\n  ".join(missing_fields)
    )

    # The detector-query site: the extraction tripwire fires inside
    # _build_inventory; here the rendered statement must be among the
    # prepared.
    assert any(_STRANDED_SOURCE in names for names in inventory.values()), (
        "the stranded-jobs detector query did not reach the prepared inventory"
    )

    # Erosion floors: a walk that silently discovers nothing, or an
    # inventory that quietly stops rendering, fails here. Floors only fail
    # downward - new statements raise the count, never break the pin.
    assert len(discovered) >= 85, (
        f"the walk found only {len(discovered)} constants (floor 85) - "
        "discovery is broken or statements were removed"
    )
    assert len(inventory) >= 100, (
        f"only {len(inventory)} distinct rendered statements (floor 100) - "
        "the render path is dropping statements"
    )

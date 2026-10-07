"""The render cache's attack surface: aliasing, boundedness, bypass, races.

An audit hardening pass over ``tests/test_sweep_sql_render_cache.py``'s
byte-equality pins.  A memoised render is correct only if the cache key
covers EVERY input the render consumes; these pins attack the four ways
that fails, plus the race and the init-time render the commit also
touched:

* KEY COMPLETENESS: the key is ``(template, schema)``.  The census pins
  prove each converted template's format-field set is exactly
  ``{schema}`` (the event-TTL template plus the module constant
  ``outbox_multiplier``), so no other caller value can reach
  ``str.format`` -- if a template ever gains a field, the census reds
  and the key must grow with it.  The end-to-end pins drive the PUBLIC
  sweep functions with a recording conn for two schemas on a warm cache
  and assert each call's SQL interpolates the CALL's schema: the
  cross-tenant worst case (schema A's rows named in schema B's query)
  reds here.
* BYPASS: an invalid schema must raise even when the cache is warm for
  the same template, a failed validation must cache nothing, and the
  check must live INSIDE the memoised function (a rejecting regex still
  rejects a fresh schema).  The event-TTL base helper's validation is
  pinned separately -- it was the unpinned one (stripping it left the
  original 46 pins green).
* BOUNDEDNESS: both caches are LRU-bounded (``maxsize=64``), and every
  call site passes a module-level constant template (AST-scanned), so
  no derived per-call template string can grow the cache unboundedly.
* RACES: concurrent cold misses on one key all return byte-identical
  text (the double-render is harmless because the render is pure); no
  mutable state rides the render.
* INIT-TIME RENDER: ``PostgresBackend`` renders the two heartbeat
  statements at ``__init__`` from its own validated ``_schema_name``
  snapshot.  The pins hold the render to that snapshot -- not to live
  settings -- so a post-init settings mutation cannot make the
  heartbeat SQL disagree with the schema every other backend statement
  was rendered from.
"""

import ast
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from pathlib import Path
from string import Formatter

import pytest

import taskq.constants as constants
from taskq.backend import _sweeps as sweeps
from taskq.backend._sql import INSERT_EVENTS_DETAIL_BATCH_SQL
from taskq.backend._sweeps import (
    _SWEEP_1_SQL,
    _SWEEP_2_SQL,
    _SWEEP_3_SQL,
    _SWEEP_4_SQL,
    _SWEEP_IDLE_KEYED_BUCKETS_SQL,
    _SWEEP_IDLE_KEYED_SLOTS_SQL,
    _SWEEP_RESULT_TTL_SQL,
    _render_event_ttl_base_sql,
    _render_sweep_sql,
)
from taskq.backend.clock import SystemClock
from taskq.backend.postgres import (
    UPDATE_JOBS_LOCK_SQL_TEMPLATE,
    UPDATE_RESERVATION_LEASES_SQL_TEMPLATE,
    PostgresBackend,
)

#: The census as plain (name, template, fields) tuples, for in-test
#: iteration (ParameterSet is not a public unpacking API).
TEMPLATES_AND_FIELDS_PLAIN: list[tuple[str, str, frozenset[str]]] = [
    ("_SWEEP_1_SQL", _SWEEP_1_SQL, frozenset({"schema"})),
    ("_SWEEP_1_ATTEMPTS_BATCH_SQL", sweeps._SWEEP_1_ATTEMPTS_BATCH_SQL, frozenset({"schema"})),
    ("_SWEEP_2_SQL", _SWEEP_2_SQL, frozenset({"schema"})),
    ("_SWEEP_2_ATTEMPTS_BATCH_SQL", sweeps._SWEEP_2_ATTEMPTS_BATCH_SQL, frozenset({"schema"})),
    ("_SWEEP_3_SQL", _SWEEP_3_SQL, frozenset({"schema"})),
    ("_SWEEP_4_SQL", _SWEEP_4_SQL, frozenset({"schema"})),
    ("_SWEEP_RESULT_TTL_SQL", _SWEEP_RESULT_TTL_SQL, frozenset({"schema"})),
    # The guarded statement's UNGUARDED base (the rolling-deploy
    # tolerance's fallback template — the same (template, schema) key
    # shape, the same single field).
    (
        "_SWEEP_RESULT_TTL_BASE_SQL",
        sweeps._SWEEP_RESULT_TTL_BASE_SQL,  # pyright: ignore[reportPrivateUsage]  # Why: the census pins the module's own template constants by name.
        frozenset({"schema"}),
    ),
    ("_SWEEP_IDLE_KEYED_BUCKETS_SQL", _SWEEP_IDLE_KEYED_BUCKETS_SQL, frozenset({"schema"})),
    ("_SWEEP_IDLE_KEYED_SLOTS_SQL", _SWEEP_IDLE_KEYED_SLOTS_SQL, frozenset({"schema"})),
    ("INSERT_EVENTS_DETAIL_BATCH_SQL", INSERT_EVENTS_DETAIL_BATCH_SQL, frozenset({"schema"})),
    (
        "_SWEEP_EVENT_TTL_SQL",
        sweeps._SWEEP_EVENT_TTL_SQL,
        frozenset({"schema", "outbox_multiplier"}),
    ),
]

#: The same census as pytest params (readable ids, not the template text).
TEMPLATES_AND_FIELDS = [
    pytest.param(name, template, fields, id=name)
    for name, template, fields in TEMPLATES_AND_FIELDS_PLAIN
]

_FORMATTER = Formatter()


@pytest.mark.parametrize(("name", "template", "fields"), TEMPLATES_AND_FIELDS)
def test_template_format_field_census(name: str, template: str, fields: frozenset[str]) -> None:
    """Every converted template consumes exactly the fields the key covers.

    This is the key-completeness proof at the source: the render's only
    caller-supplied input is the schema (in the key); the only other
    field anywhere is ``outbox_multiplier``, a module constant folded
    into the event-TTL helper.  A template edited to consume a new
    caller value reds here BEFORE the cache can serve the first
    caller's value to everyone.
    """
    consumed = {field for _, field, _, _ in _FORMATTER.parse(template) if field is not None}
    assert consumed == set(fields), f"{name} consumes {consumed}, key covers {set(fields)}"


@pytest.mark.parametrize(("name", "template", "fields"), TEMPLATES_AND_FIELDS)
def test_render_serves_each_schema_its_own_bytes(
    name: str, template: str, fields: frozenset[str]
) -> None:
    """Interleaved warm-cache calls: no schema ever sees another's SQL.

    The worst-case cache bug is aliasing -- the key missing an input
    that varies per call, so caller B gets caller A's statement.  With
    the key proven complete (census above), the observable form is
    pinned here on a cache warmed by BOTH schemas in interleaved order.
    """
    a, b = "tq_tenant_a", "tq_tenant_b"
    direct_a: str
    direct_b: str
    if "outbox_multiplier" in fields:
        direct_a = template.format(
            schema=a, outbox_multiplier=sweeps.RECLAIM_OUTBOX_RETENTION_MULTIPLIER
        )
        direct_b = template.format(
            schema=b, outbox_multiplier=sweeps.RECLAIM_OUTBOX_RETENTION_MULTIPLIER
        )
        render_a = _render_event_ttl_base_sql(a)
        render_b = _render_event_ttl_base_sql(b)
    else:
        direct_a = template.format(schema=a)
        direct_b = template.format(schema=b)
        render_a = _render_sweep_sql(template, a)
        render_b = _render_sweep_sql(template, b)
    for _ in range(2):  # warm: the second pass must be served from the cache
        if "outbox_multiplier" in fields:
            assert _render_event_ttl_base_sql(a) is render_a
            assert _render_event_ttl_base_sql(b) is render_b
        else:
            assert _render_sweep_sql(template, a) is render_a
            assert _render_sweep_sql(template, b) is render_b
    assert a in render_a and b not in render_a
    assert b in render_b and a not in render_b
    assert render_a == direct_a
    assert render_b == direct_b
    assert render_a != render_b


# ── End-to-end: the public sweep functions on a warm cache ──────────


class _NullTransaction:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, *exc: object) -> None:
        return None


class _RecordingConn:
    """Duck-typed ConnLike recording every SQL text it is handed.

    ``fetch`` answers the statement_timeout probe (a mapping row) and
    returns no sweep rows; ``execute`` returns a zero-change tag, so
    each sweep runs its full render path and stops.
    """

    def __init__(self) -> None:
        self.sqls: list[str] = []

    def transaction(self) -> _NullTransaction:
        return _NullTransaction()

    async def fetch(self, sql: str, *args: object) -> list[dict[str, str]]:
        if "current_setting" not in sql:
            self.sqls.append(sql)
        if "current_setting" in sql:
            return [{"current_setting": "5s"}]
        return []

    async def execute(self, sql: str, *args: object) -> str:
        self.sqls.append(sql)
        return "UPDATE 0"


#: The public sweeps whose conn surface ``_RecordingConn`` satisfies,
#: with the keyword args each signature demands (all bind-parameter
#: values -- none of them ever reaches ``str.format``).
PUBLIC_SWEEPS = [
    ("sweep_scheduled_to_pending", sweeps.sweep_scheduled_to_pending),
    ("sweep_leaked_reservation_slots", sweeps.sweep_leaked_reservation_slots),
    ("sweep_expired_results", sweeps.sweep_expired_results),
    ("sweep_idle_keyed_rows", sweeps.sweep_idle_keyed_rows),
    ("sweep_expired_events", sweeps.sweep_expired_events),
]


@pytest.mark.parametrize(("sweep_name", "sweep_fn"), PUBLIC_SWEEPS)
async def test_public_sweep_renders_the_calling_schema_on_a_warm_cache(
    sweep_name: str, sweep_fn: object
) -> None:
    """Each public sweep call hands the conn SQL naming ITS OWN schema.

    Called back-to-back with two schemas, the second on a cache the
    first warmed: a cross-schema serve anywhere between the call site
    and ``conn`` lands two identical (or swapped) statements here.
    """
    conn = _RecordingConn()
    kwargs: dict[str, object] = {"batch_size": 8}
    if sweep_name == "sweep_scheduled_to_pending":
        kwargs["statement_timeout_ms"] = 5_000
    if sweep_name == "sweep_idle_keyed_rows":
        kwargs["horizon"] = timedelta(days=1)
    if sweep_name == "sweep_expired_events":
        kwargs["retention"] = timedelta(days=1)

    await sweep_fn(conn, schema="tq_tenant_a", **kwargs)  # type: ignore[operator]
    sqls_a = list(conn.sqls)
    conn.sqls.clear()
    await sweep_fn(conn, schema="tq_tenant_b", **kwargs)  # type: ignore[operator]
    sqls_b = list(conn.sqls)

    assert sqls_a and sqls_b, f"{sweep_name} issued no statements"
    assert len(sqls_a) == len(sqls_b)
    # The schema-bearing statements (the set_config GUC probes carry no
    # schema) must each name exactly the calling schema -- and at least
    # one statement per call must be schema-bearing.
    schema_bearing_a = [sql for sql in sqls_a if "tq_tenant" in sql]
    schema_bearing_b = [sql for sql in sqls_b if "tq_tenant" in sql]
    assert schema_bearing_a and schema_bearing_b, f"{sweep_name} rendered no schema"
    for sql_a, sql_b in zip(schema_bearing_a, schema_bearing_b, strict=True):
        assert "tq_tenant_a" in sql_a and "tq_tenant_b" not in sql_a
        assert "tq_tenant_b" in sql_b and "tq_tenant_a" not in sql_b
        # Same statement shape, only the schema differs.
        assert sql_a.replace("tq_tenant_a", "X") == sql_b.replace("tq_tenant_b", "X")


# ── Validation bypass ────────────────────────────────────────────────


def test_invalid_schema_raises_on_a_warm_cache() -> None:
    """A cached valid render must not bypass the new call's validation.

    The warm entry is for a DIFFERENT key (the schema is IN the key), so
    an invalid schema can never be served one -- and a failed
    validation must leave the cache untouched, so no exception is ever
    memoised either.
    """
    info_before = _render_sweep_sql.cache_info()
    with pytest.raises(ValueError, match="invalid schema identifier"):
        _render_sweep_sql(_SWEEP_3_SQL, "warm'; DROP SCHEMA x")
    info_after = _render_sweep_sql.cache_info()
    assert info_after.currsize == info_before.currsize
    assert info_after.misses == info_before.misses + 1
    # The valid entry still serves, byte-identical, after the rejection.
    assert _render_sweep_sql(_SWEEP_3_SQL, "tq_warm_pin") == _SWEEP_3_SQL.format(
        schema="tq_warm_pin"
    )


def test_event_ttl_base_invalid_schema_raises_per_call() -> None:
    """The event-TTL base helper's identifier check, pinned like the
    main render's: an invalid schema raises, twice, cache warm or not.

    Mutation-proven load-bearing: stripping this check left every pin in
    ``test_sweep_sql_render_cache.py`` green -- this pin is what reds.
    """
    for _ in range(2):
        with pytest.raises(ValueError, match="invalid schema identifier"):
            _render_event_ttl_base_sql("ev'; DROP SCHEMA x")


def test_event_ttl_base_validates_on_a_warm_cache() -> None:
    warm = _render_event_ttl_base_sql("tq_ttl_warm")
    with pytest.raises(ValueError, match="invalid schema identifier"):
        _render_event_ttl_base_sql("ev; --")
    assert _render_event_ttl_base_sql("tq_ttl_warm") is warm


def test_validation_lives_inside_the_memoised_function(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A rejecting regex still rejects a FRESH schema: the check runs in
    the cached body, it is not hoisted to some pre-cache call site.
    (The regex is restored by monkeypatch; a fresh schema misses the
    cache either way, so the body -- and its check -- must run.)"""

    class _RejectAll:
        def match(self, schema: str) -> None:
            return None

    # require_schema() (taskq.constants) owns the validation now; it reads
    # _IDENT_RE from the constants module at call time, so the rejecting
    # regex must be patched THERE, not on _sweeps.
    monkeypatch.setattr(constants, "_IDENT_RE", _RejectAll())
    with pytest.raises(ValueError, match="invalid schema identifier"):
        _render_sweep_sql(_SWEEP_3_SQL, "tq_fresh_schema")


# ── Boundedness ──────────────────────────────────────────────────────


def test_render_caches_are_lru_bounded() -> None:
    """Both caches carry a finite maxsize: no unbounded growth on a
    long-lived leader, whatever key volume the process sees."""
    assert _render_sweep_sql.cache_info().maxsize == 64
    assert _render_event_ttl_base_sql.cache_info().maxsize == 64


def test_cache_stays_bounded_under_many_schemas() -> None:
    """200 distinct schemas across all 11 templates: the LRU bound
    holds (entries evict, correctness of a hit is unaffected)."""
    for i in range(200):
        schema = f"tq_bound_{i}"
        for _name, template, fields in TEMPLATES_AND_FIELDS_PLAIN:
            if "outbox_multiplier" in fields:
                _render_event_ttl_base_sql(schema)
            else:
                _render_sweep_sql(template, schema)
    assert _render_sweep_sql.cache_info().currsize <= 64
    assert _render_event_ttl_base_sql.cache_info().currsize <= 64


def test_call_sites_pass_module_constants_not_composed_templates() -> None:
    """AST census: every ``_render_sweep_sql`` call site's template
    argument is a bare module-constant Name.

    The unbounded-growth vector is a caller composing a derived
    template string per schema/table (an f-string variant): each new
    text would be a new cache key.  No such composition exists, and
    this scan reds the moment one is added.
    """
    source = Path(sweeps.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "_render_sweep_sql"
    ]
    # 10 direct call sites: the collapse of the two single-statement
    # sweeps into _run_single_statement_sweep folded their two calls
    # into the helper's one (fed by its ``sql_template`` parameter,
    # pinned to module constants by the scan below).
    assert len(calls) == 10, (
        f"expected the 10 direct call sites (INSERT_EVENTS_DETAIL_BATCH_SQL "
        f"renders at two sweeps; the two single-statement sweeps render via "
        f"_run_single_statement_sweep), found {len(calls)}"
    )
    for call in calls:
        arg = call.args[0]
        if isinstance(arg, ast.Name) and arg.id == "sql_template":
            # The shared helper's own call: its parameter is pinned to
            # module constants at the helper's call sites (scan below).
            continue
        assert isinstance(arg, ast.Name), (
            f"line {call.lineno}: template argument is composed "
            f"({type(arg).__name__}), the (template, schema) key would grow per call"
        )
        assert arg.id in {
            "_SWEEP_1_SQL",
            "_SWEEP_1_ATTEMPTS_BATCH_SQL",
            "_SWEEP_2_SQL",
            "_SWEEP_2_ATTEMPTS_BATCH_SQL",
            "_SWEEP_3_SQL",
            "_SWEEP_4_SQL",
            "_SWEEP_RESULT_TTL_SQL",
            "_SWEEP_IDLE_KEYED_BUCKETS_SQL",
            "_SWEEP_IDLE_KEYED_SLOTS_SQL",
            "INSERT_EVENTS_DETAIL_BATCH_SQL",
        }

    # The indirection must not become a bypass: every
    # _run_single_statement_sweep call site's ``sql_template`` argument
    # is itself a bare module-constant Name, so the (template, schema)
    # cache key still only ever sees module constants.
    helper_calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "_run_single_statement_sweep"
    ]
    assert len(helper_calls) >= 2, (
        f"expected the 2 collapsed single-statement sweeps to delegate to "
        f"_run_single_statement_sweep, found {len(helper_calls)}"
    )
    for call in helper_calls:
        kw = {k.arg: k.value for k in call.keywords}
        arg = kw.get("sql_template")
        assert isinstance(arg, ast.Name), (
            f"line {call.lineno}: helper sql_template argument is composed "
            f"({type(arg).__name__}), the (template, schema) key would grow per call"
        )
        assert arg.id in {
            "_SWEEP_4_SQL",
            "_SWEEP_RESULT_TTL_SQL",
            "_SWEEP_RESULT_TTL_BASE_SQL",
        }, f"line {call.lineno}: helper sql_template is {arg.id}, not a pinned module constant"


# ── Races ────────────────────────────────────────────────────────────


def test_concurrent_cold_misses_agree_on_bytes() -> None:
    """Threads racing the same cold key: every result byte-identical.

    lru_cache's miss path may render twice under a race; the render is
    pure, so the losers must agree with the winner to the byte -- any
    mutable state riding the render would show up as a divergent copy.
    """
    schema = "tq_race_pin"
    direct = _SWEEP_2_SQL.format(schema=schema)
    with ThreadPoolExecutor(max_workers=16) as pool:
        results = list(pool.map(lambda _: _render_sweep_sql(_SWEEP_2_SQL, schema), range(64)))
    assert all(r == direct for r in results)
    assert all(r is results[0] for r in results)


def test_concurrent_misses_on_distinct_schemas_stay_disjoint() -> None:
    """Threads rendering DISTINCT schemas concurrently: each caller gets
    its own schema's text, never a neighbour's (the aliasing race)."""
    schemas = [f"tq_race_{i}" for i in range(16)]
    with ThreadPoolExecutor(max_workers=16) as pool:
        results = list(pool.map(lambda s: _render_sweep_sql(_SWEEP_2_SQL, s), schemas))
    for schema, render in zip(schemas, results, strict=True):
        assert schema in render
        assert render == _SWEEP_2_SQL.format(schema=schema)


# ── The init-time heartbeat render ───────────────────────────────────


class _InitSettings:
    def __init__(self, schema_name: str) -> None:
        self.schema_name = schema_name


class _InitDeps:
    def __init__(self, schema_name: str) -> None:
        self.settings = _InitSettings(schema_name)
        self.worker_pool = object()
        self.heartbeat_pool = object()


def _make_backend(schema: str) -> PostgresBackend:
    return PostgresBackend(
        _InitDeps(schema),  # pyright: ignore[reportArgumentType]  # Why: duck-typed BackendDeps; __init__ reads only settings.schema_name, mirrors test_admin_audit_trail.py's idiom.
        clock=SystemClock(),
        cancellation_grace_period=timedelta(seconds=5),
        cleanup_grace_period=timedelta(seconds=5),
    )


def test_heartbeat_sql_rendered_at_init_matches_direct_format() -> None:
    """The two init-time renders are byte-identical to the per-beat
    ``.format`` they replaced."""
    backend = _make_backend("tq_hb_init")
    assert backend._heartbeat_jobs_sql == UPDATE_JOBS_LOCK_SQL_TEMPLATE.format(  # pyright: ignore[reportPrivateUsage]
        schema="tq_hb_init"
    )
    assert backend._extend_reservation_leases_sql == (  # pyright: ignore[reportPrivateUsage]
        UPDATE_RESERVATION_LEASES_SQL_TEMPLATE.format(schema="tq_hb_init")
    )


def test_heartbeat_render_tracks_the_backend_schema_snapshot_not_live_settings() -> None:
    """The init render is frozen to ``_schema_name`` -- and so is every
    other statement on the backend, which reads the same snapshot.  A
    settings object mutated after init therefore cannot make the
    heartbeat SQL disagree with the rest of the backend's SQL: both
    stay on the init snapshot.  (The backend never re-reads
    ``deps.settings.schema_name``; the schema's lifetime is the
    backend's.)"""
    backend = _make_backend("tq_hb_snapshot")
    deps = _InitDeps("tq_hb_MUTATED")
    backend._deps = deps  # pyright: ignore[reportAttributeAccessIssue]
    assert backend._heartbeat_jobs_sql == UPDATE_JOBS_LOCK_SQL_TEMPLATE.format(  # pyright: ignore[reportPrivateUsage]
        schema=backend._schema_name  # pyright: ignore[reportPrivateUsage]
    )
    assert "tq_hb_MUTATED" not in backend._heartbeat_jobs_sql  # pyright: ignore[reportPrivateUsage]


def test_backend_init_rejects_invalid_schema_before_rendering() -> None:
    """The init-time validation still gates the init-time renders: an
    invalid schema raises the same ValueError, nothing rendered."""
    with pytest.raises(ValueError, match="invalid schema identifier"):
        _make_backend("hb'; DROP SCHEMA x")

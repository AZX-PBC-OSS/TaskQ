"""The workflow schema round's structural + measured pins (T03, §16.4).

Structural (the lock-scope family's form): the three-file single-lock-class
split, the seam-only id generation in the DDL, the phase-obligations
header, the additive-only rule (every file is a ``pre_`` file — no
workflow ``post_`` ever ships for v1), and the dispatch-claim exclusion
clause's presence at every claimable-row site. The IMPORT pin (§16.1's
law) runs on the AST harness (``tests/_import_discipline.py``) + a fresh
interpreter, with the mutated-``__init__`` red drill.

Measured (PG): pin 1 — both table shapes built, 10k seeded rows, the
heap/tuple deltas ≤ 8 B (the red drill's misaligned padding blows it);
pin 2 — each workflow partial index ≤ 1% of a same-column full index
(the 100k-row clone carries the volume; the full control index doubles as
the red drill's convicted shape).
"""

# Why: the f-string SQL interpolates only the fixture's own throwaway schema identifiers (validated by the migration runner's _IDENT_RE) or renders the bundled migration files; all values are $n-bound.

from __future__ import annotations

import json
import subprocess
import sys
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import asyncpg
import pytest

from taskq import migrate as migrate_mod
from taskq._ids import new_base62, new_uuid
from taskq.migrate import split_statements
from tests._wf_fixtures import MEASUREMENTS

WORKFLOW_ROUND = "01.00.24"
_MIGRATIONS_DIR = Path(__file__).parent.parent / "src" / "taskq" / "migrations"
_ROUND_FILES = sorted(_MIGRATIONS_DIR.glob(f"{WORKFLOW_ROUND}_*.sql"))
_WF_COLUMNS = ("parent_id", "deps_pending", "map_index", "step_key", "code_version")
_SEED_ROWS = 10_000


def _write_measurement(name: str, payload: object) -> None:
    MEASUREMENTS.mkdir(exist_ok=True)
    (MEASUREMENTS / name).write_text(json.dumps(payload, indent=2, default=str))


# ── Structural pins: the single-lock-class split (family 1's form) ──────


def _round_files() -> list[Path]:
    assert _ROUND_FILES, f"the {WORKFLOW_ROUND} round is missing"
    return _ROUND_FILES


def _round_by_name(name: str) -> Path:
    """The workflow round's file BY NAME (the family also carries LIB-2's
    parent_id file at 01.00.23_01 — positional indices died with the
    consolidation's four-file family)."""
    found = [f for f in _round_files() if f.name == name]
    assert found, f"{name} missing from the {WORKFLOW_ROUND} round"
    return found[0]


def test_round_is_split_into_single_lock_class_files() -> None:
    """The three files apply in order and each file is single-lock-class:
    the columns file ONLY metadata-only ALTERs, the tables file ONLY CREATE
    TABLEs, the index file ONLY CREATE INDEXes. The runner wraps each FILE
    in one transaction, so a file's statements share ONE write-block
    window — mixed lock classes in one file are the convicted shape (the
    lock-scope family's finding)."""
    names = [f.name for f in _round_files()]
    assert names == [
        "01.00.24_01_pre_workflow_columns.sql",
        "01.00.24_02_pre_workflow_tables.sql",
        "01.00.24_03_pre_workflow_indexes.sql",
    ], names
    # LIB-2's fan-out ledger rides main's own 01.00.23_01 identity (the
    # consolidation kept it: deployed ledgers already carry it); its index
    # is SPLIT out to 01.00.23_07 by the single-lock-class law (ALTERs and
    # CREATE INDEX are different lock classes — the family-1 pin convicted
    # the mixed file on the consolidation's own first run). The split's
    # teeth: 23_01 holds ONLY the two ADD COLUMNs, 23_07 ONLY the index.
    lib2 = _MIGRATIONS_DIR / "01.00.23_01_pre_jobs_parent_id.sql"
    split = _MIGRATIONS_DIR / "01.00.23_07_pre_jobs_parent_pending_idx.sql"
    lib2_statements = [l for l in lib2.read_text().splitlines() if l.startswith(("ALTER TABLE", "CREATE INDEX"))]
    assert lib2_statements == [
        'ALTER TABLE "{schema}".jobs ADD COLUMN parent_id uuid;',
        'ALTER TABLE "{schema}".jobs_archive ADD COLUMN parent_id uuid;',
    ], lib2_statements
    assert [l for l in split.read_text().splitlines() if l.startswith("CREATE INDEX")] != [], (
        "01.00.23_07 must carry the fan-out ledger's index (the split's own point)"
    )


def test_every_workflow_file_is_a_pre_file() -> None:
    """ALL workflow DDL is additive — no ``post_`` ever ships for v1 (the
    forever rule): a pre-workflow worker reads the row shape unchanged."""
    for path in _round_files():
        assert "_pre_" in path.name, path


def _statement_bodies(sql: str) -> list[str]:
    bodies: list[str] = []
    for stmt in split_statements(sql):
        lines = stmt.splitlines()
        i = 0
        while i < len(lines) and (not lines[i].strip() or lines[i].strip().startswith("--")):
            i += 1
        bodies.append("\n".join(lines[i:]))
    return [b for b in bodies if b.strip()]


def _statement_kinds(sql: str) -> list[str]:
    kinds: list[str] = []
    for body in _statement_bodies(sql):
        collapsed = " ".join(body.split()).upper().replace('"', "")
        if collapsed.startswith("CREATE UNIQUE INDEX") or collapsed.startswith("CREATE INDEX"):
            kinds.append("CREATE INDEX")
        elif collapsed.startswith("CREATE TABLE"):
            kinds.append("CREATE TABLE")
        elif collapsed.startswith("ALTER TABLE"):
            kinds.append("ALTER TABLE")
        elif collapsed.startswith("COMMENT ON"):
            kinds.append("COMMENT ON")
        else:
            kinds.append(collapsed.split(" ")[0])
    return kinds


def test_columns_file_holds_only_metadata_alters() -> None:
    sql = (_round_by_name("01.00.23_04_pre_workflow_columns.sql")).read_text()
    kinds = _statement_kinds(sql)
    # COMMENT ON is metadata-only (a catalog write; the columns file's
    # documentation column comments ride the same instant lock) - allowed
    # beside the ALTERs.
    assert kinds and set(kinds) <= {"ALTER TABLE", "COMMENT ON"}, kinds
    assert "ALTER TABLE" in kinds, kinds
    for body in _statement_bodies(sql):
        assert "ADD COLUMN" in body.upper() or body.upper().startswith("COMMENT"), body
    for column in _WF_COLUMNS:
        assert column in sql, f"the columns file must carry {column}"


def test_tables_file_holds_only_create_tables() -> None:
    sql = (_round_by_name("01.00.23_05_pre_workflow_tables.sql")).read_text()
    kinds = _statement_kinds(sql)
    assert set(kinds) <= {"CREATE TABLE", "COMMENT ON"}, kinds
    for table in ("wf_edge", "wf_join_fire", "wf_outbox", "wf_step_ledger"):
        assert f'CREATE TABLE "{{schema}}".{table}' in sql, table


def test_indexes_file_holds_only_create_indexes() -> None:
    sql = (_round_by_name("01.00.23_06_pre_workflow_indexes.sql")).read_text()
    kinds = _statement_kinds(sql)
    assert set(kinds) == {"CREATE INDEX"}, kinds
    # THE PARTIAL-INDEX DOCTRINE: the workflow-scoped indexes are partial;
    # the measured exemption is the point (pin 2 measures it).
    create_ons = [b for b in _statement_bodies(sql) if b.upper().startswith("CREATE")]
    partial = [b for b in create_ons if " WHERE " in b.upper()]
    assert len(partial) >= 3, "the workflow indexes must be partial"


def test_workflow_ddl_ids_are_seam_only() -> None:
    """T04 pin 10's DDL half: the workflow DDL carries NO ``gen_random_uuid``
    and NO ``uuid4`` — every id is app-side through the ``taskq._ids`` seam
    (uuid7, the TID251 discipline). Any hit reds."""
    for path in _round_files():
        # The STATEMENT bodies are checked, not the comments: the header
        # documents the ban (it names the banned spellings), the DDL must
        # never USE them.
        for body in _statement_bodies(path.read_text()):
            assert "gen_random_uuid" not in body.lower(), (path, body[:80])
            assert "uuid4" not in body.lower(), (path, body[:80])


def test_phase_obligations_header_present() -> None:
    for path in _round_files():
        if path.name in (
            "01.00.23_01_pre_jobs_parent_id.sql",
            "01.00.23_07_pre_jobs_parent_pending_idx.sql",
        ):
            # LIB-2's own file (main's shipped identity, authored before
            # the round-split discipline): its header documents its own
            # lock classes in its own voice — the PHASE OBLIGATIONS
            # header is the WORKFLOW round's convention.
            continue
        header = path.read_text()[:2500].upper()
        assert "PHASE OBLIGATIONS" in header, path


def test_dispatch_claim_carries_the_join_wait_exclusion() -> None:
    """The dispatch-exclusion clause (T03's owning decision): the claim
    statement's WHERE carries ``deps_pending = 0`` at every claimable-row
    site — a semantic no-op for vanilla rows (DEFAULT 0), the thing that
    keeps a join-wait row unclaimable."""
    dispatch = (
        Path(__file__).parent.parent / "src" / "taskq" / "backend" / "_dispatch_sql.py"
    ).read_text()
    # The candidate scans + the terminal race guard + the capacity probes.
    assert dispatch.count("deps_pending = 0") >= 12, dispatch.count("deps_pending = 0")
    # The claimable probe (the "should this round run at all" check) too.
    assert "AND j.deps_pending = 0" in dispatch


# ── Pin 3: the IMPORT pin (the §16.1 law, on the AST harness) ────────────


def test_import_law_taskq_never_imports_workflows() -> None:
    """A fresh interpreter's ``import taskq`` must leave ``taskq.workflows``
    absent from ``sys.modules``; the AST harness (module-scope invariants
    parsed, not grepped) convicts the package ``__init__`` itself."""
    import taskq
    from tests import _import_discipline

    offenders = _import_discipline.couples_to_at_import_time(taskq, "taskq.workflows")
    assert not offenders, (
        f"taskq/__init__.py imports taskq.workflows at module scope: {offenders} "
        "(the §16.1 import law: import taskq never imports the package)"
    )

    out = subprocess.run(  # Why: the fresh-interpreter probe runs the same venv's interpreter with a fixed argv.
        [
            sys.executable,
            "-c",
            "import taskq, sys, json; "
            "print(json.dumps({'violates': 'taskq.workflows' in sys.modules}))",
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=60,
    ).stdout
    assert '"violates": false' in out, out


#: §16.1's BOOT SURFACES (F-R5's widening): every process entry the
#: package ships — the library root, the worker's boot entry (the
#: intercept seam), the CLI, the web assembly, and the admin's workflow
#: PAGES module. The pin's old probe checked ONLY ``taskq/__init__``, so
#: the stated law and its enforcement didn't match the shipped tree:
#: ``web/admin/_wf_rows.py`` imported the package at module scope (and
#: ``web/admin/workflows.py`` imported _wf_rows) with every pin green.
#: Each surface is probed in a FRESH interpreter — the runtime truth
#: (``taskq.workflows`` absent from ``sys.modules``), not a grep.
_IMPORT_LAW_SURFACES = (
    "import taskq",
    "import taskq.worker.run",
    "import taskq.cli",
    "import taskq.web",
    "import taskq.web.admin.workflows",
)


@pytest.mark.parametrize("surface", _IMPORT_LAW_SURFACES)
def test_import_law_every_boot_surface_never_imports_workflows(surface: str) -> None:
    """The §16.1 law, widened (F-R5): importing ANY boot surface must
    leave ``taskq.workflows`` unloaded — a module-scope workflows import
    anywhere in the surface's transitive tree is convicted. The admin's
    page modules are the proven violator (lazy-discovery loads them only
    when the pages render; their seams import lazily — the
    ``_wf_actions`` pattern)."""
    out = subprocess.run(  # noqa: S603 # Why: the fresh-interpreter probe runs the same venv's interpreter with a fixed argv; the surface strings are the pin module's own constant tuple, never user input.
        [
            sys.executable,
            "-c",
            f"{surface}, sys, json; "
            "print(json.dumps({'surface': "
            f"\"{surface}\", 'violates': 'taskq.workflows' in sys.modules}}))",
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=60,
    ).stdout
    assert '"violates": false' in out, out


def test_import_law_red_drill(tmp_path: Path) -> None:
    """The revert drill: the pin CAN fail — a module-level import of the
    workflows package in ``taskq/__init__.py`` is convicted by the same AST
    harness (the fixture mutation, parsed in isolation; the RED output is
    the offender list). The mutated fixture module's scratch file rides
    pytest's ``tmp_path`` — never the repo tree (a stray untracked module
    turns a repo-wide lint red)."""
    import ast
    import types

    import taskq
    from tests import _import_discipline

    real_init = Path(taskq.__file__).read_text()
    mutated = "import taskq.workflows\n" + real_init
    fixture = types.ModuleType("taskq_fixture_mutated_init")
    scratch = tmp_path / "fixture_mutated_init.py"
    fixture.__dict__["__file__"] = str(scratch)
    # Point the harness's source getter at the mutated text: the harness
    # parses module source, so the drill parses the MUTATED source and asks
    # the same question the pin asks.
    scratch.write_text(mutated)
    tree = ast.parse(mutated)
    names = {
        name
        for node in tree.body
        if isinstance(node, ast.Import | ast.ImportFrom)
        for name in _import_discipline._imported_names(node)  # pyright: ignore[reportPrivateUsage]  # Why: the drill reuses the harness's own parser on a synthetic tree.
    }
    # RED: the mutated module-level import is present — the same check that
    # stays clean (zero offenders) on the real package above.
    assert "taskq.workflows" in names
    assert not _import_discipline.couples_to_at_import_time(taskq, "taskq.workflows"), (
        "the real package must stay clean while the fixture proves the drill fires"
    )
    del fixture


# ── PG-backed pins 1 and 2 ───────────────────────────────────────────────


def _pre_round_migrations() -> list[str]:
    """Every bundled migration key BEFORE the workflow round — the pre-
    workflow row shape is the shape those files build (the control side of
    pin 1's comparison)."""
    return [
        m.key
        for m in sorted(migrate_mod.discover(), key=lambda m: m.key)
        if m.key < f"{WORKFLOW_ROUND}_01:pre"
    ]


async def _apply_selected(conn: asyncpg.Connection, schema: str, keys: set[str]) -> None:
    """Apply the selected migrations' files, in discovery order, each file's
    statements inside one transaction (the runner's own wrap discipline)."""
    await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    await conn.execute(f'CREATE SCHEMA "{schema}"')
    for migration in migrate_mod.discover():
        if migration.key not in keys:
            continue
        rendered = migration.render(schema)
        async with conn.transaction():
            for stmt in split_statements(rendered):
                await conn.execute(stmt)


async def _seed_rows(conn: asyncpg.Connection, target: str, rows: int) -> None:
    await conn.executemany(
        f'INSERT INTO "{target}".jobs '
        "(id, actor, queue, payload, max_attempts, retry_kind) "
        "VALUES ($1, $2, $3, $4, $5, $6)",
        [(new_uuid(), "bench", "default", '{"pin": 1}', 3, "transient") for _ in range(rows)],
    )


@pytest.fixture(scope="module")
async def width_schemas(pg_dsn: str) -> AsyncIterator[dict[str, str]]:
    """The extended shape (the full bundled set, the round applied) vs the
    pre-round control shape (every migration BEFORE the workflow round),
    seeded identically; plus the RED-DRILL shape (the five columns PLUS
    deliberately-misaligned padding columns)."""
    suffix = new_base62(8, precision="random").lower()
    extended = f"t03x_{suffix}"
    control = f"t03c_{suffix}"
    padded = f"t03p_{suffix}"
    conn = await asyncpg.connect(pg_dsn)
    try:
        for target in (extended, control, padded):
            await conn.execute(f'DROP SCHEMA IF EXISTS "{target}" CASCADE')
        from taskq.migrate import apply_pending

        await apply_pending(conn, schema=extended)
        pre_round = set(_pre_round_migrations())
        await _apply_selected(conn, control, pre_round)
        # The red-drill shape: the pre-round table + the round's five
        # columns + three boolean padding columns (forced attribute
        # headers - the misalignment the bound must catch).
        await _apply_selected(conn, padded, pre_round)
        column_types = {
            "parent_id": "uuid",
            "deps_pending": "smallint NOT NULL DEFAULT 0",
            "map_index": "smallint",
            "step_key": "text",
            "code_version": "text",
        }
        alters = [
            f'ALTER TABLE "{padded}".jobs ADD COLUMN IF NOT EXISTS {c} {column_types[c]}'
            for c in _WF_COLUMNS
        ] + [
            # The misalignment: a wide text column with a per-row value --
            # bytes alignment cannot absorb, the +16 B/row the drill needs.
            f'ALTER TABLE "{padded}".jobs '
            "ADD COLUMN IF NOT EXISTS pad_probe text NOT NULL DEFAULT "
            "'0123456789abcdef'"
        ]
        for alter in alters:
            await conn.execute(alter)

        for target in (extended, control, padded):
            await _seed_rows(conn, target, _SEED_ROWS)
        for target in (extended, control, padded):
            await conn.execute(f'VACUUM ANALYZE "{target}".jobs')
        yield {
            "dsn": pg_dsn,
            "extended": extended,
            "control": control,
            "padded": padded,
        }
    finally:
        for target in (extended, control, padded):
            await conn.execute(f'DROP SCHEMA IF EXISTS "{target}" CASCADE')
        await conn.close()


async def _shape_stats(conn: asyncpg.Connection, target: str, _rows: int) -> dict[str, float]:
    # The divisor is the LIVE row count (not the seed constant): a sibling
    # pin may have bulk-grown the table (test order shuffles), and the
    # heap-bytes/row measurement must divide the heap by the rows actually
    # in it.
    row = await conn.fetchrow(
        f"""
        SELECT
            pg_relation_size('"{target}".jobs')::float / GREATEST(
                (SELECT count(*) FROM "{target}".jobs), 1
            ) AS heap_per_row,
            (SELECT avg(pg_column_size(j)) FROM "{target}".jobs j) AS avg_tuple
        """
    )
    assert row is not None
    return {"heap_per_row": row["heap_per_row"], "avg_tuple": row["avg_tuple"]}


async def test_pin_1_row_width_residual(width_schemas: dict[str, str]) -> None:
    """THE ROW-WIDTH PIN: heap-bytes/row delta ≤ 8 AND avg-tuple delta ≤ 8
    (the five NULL/0 columns must ride MAXALIGN padding — the measured
    residuals are +3.0 B tuple / 0.0 B heap; re-measured here, never
    assumed)."""
    conn = await asyncpg.connect(width_schemas["dsn"])
    try:
        vanilla = await _shape_stats(conn, width_schemas["control"], _SEED_ROWS)
        extended = await _shape_stats(conn, width_schemas["extended"], _SEED_ROWS)
        heap_delta = extended["heap_per_row"] - vanilla["heap_per_row"]
        tuple_delta = extended["avg_tuple"] - vanilla["avg_tuple"]
        _write_measurement(
            "pin1-row-width.json",
            {
                "pin": "row-width",
                "vanilla": vanilla,
                "extended": extended,
                "heap_delta_bytes": heap_delta,
                "tuple_delta_bytes": tuple_delta,
                "bound_bytes": 8,
                "seed_rows": _SEED_ROWS,
            },
        )
        assert abs(heap_delta) <= 8, f"heap bytes/row delta {heap_delta:.2f} B exceeds 8 B"
        assert abs(tuple_delta) <= 8, f"avg-tuple delta {tuple_delta:.2f} B exceeds 8 B"
    finally:
        await conn.close()


async def test_pin_1_red_drill_misaligned_padding(width_schemas: dict[str, str]) -> None:
    """THE ROW-WIDTH PIN'S REVERT DRILL: a deliberately-misaligned padding
    fixture (three boolean columns — forced attribute headers) MUST blow
    the bound — the pin can fail. The RED output is the drill's measurement
    file; the shipped shape above stays green."""
    conn = await asyncpg.connect(width_schemas["dsn"])
    try:
        vanilla = await _shape_stats(conn, width_schemas["control"], _SEED_ROWS)
        padded = await _shape_stats(conn, width_schemas["padded"], _SEED_ROWS)
        tuple_delta = padded["avg_tuple"] - vanilla["avg_tuple"]
        _write_measurement(
            "pin1-red-pad-drift.json",
            {
                "pin": "row-width-red-drill",
                "vanilla": vanilla,
                "padded": padded,
                "tuple_delta_bytes": tuple_delta,
            },
        )
        assert tuple_delta > 8, (
            f"the red drill did not fire: misaligned padding moved the tuple by "
            f"only {tuple_delta:.2f} B — the pin cannot fail, it is a decoration"
        )
    finally:
        await conn.close()


async def test_pin_2_partial_index_exemption(width_schemas: dict[str, str]) -> None:
    """THE PARTIAL-INDEX PIN: each workflow partial index ≤ 1% of a
    same-column FULL index (the measured truth: 0.5%, 194x). The volume
    lives in a CLONE of the jobs table (LIKE ... INCLUDING ALL -- the
    migration's exact index shapes) so the 100k-row bulk never pollutes
    pin 1's row-width population (test order shuffles); the clone carries
    the SAME partial/full shapes the migration ships."""
    conn = await asyncpg.connect(width_schemas["dsn"])
    clone = f"t03bulk_{width_schemas['extended']}"
    try:
        schema = width_schemas["extended"]
        await conn.execute(f'DROP TABLE IF EXISTS "{schema}"."{clone}"')
        await conn.execute(
            f'CREATE TABLE "{schema}"."{clone}" (LIKE "{schema}".jobs INCLUDING ALL)'
        )
        # Scale to the evidence's shape (100k rows): the partial index's
        # fixed few-page overhead reads <1% only against a deep table.
        await conn.executemany(
            f'INSERT INTO "{schema}"."{clone}" '
            "(id, actor, queue, payload, max_attempts, retry_kind) "
            "VALUES ($1, 'bench2', 'default', '{}', 3, 'transient')",
            [(new_uuid(),) for _ in range(100_000)],
        )
        # The full control index: same COLUMN as the join-wait partial, no
        # WHERE -- the 1% comparison's honest control.
        await conn.execute(
            f'CREATE INDEX IF NOT EXISTS t03_full_control_idx ON "{schema}"."{clone}" (id)'
        )
        await conn.execute(f'ANALYZE "{schema}"."{clone}"')
        # The clone's partial indexes auto-named (LIKE INCLUDING ALL) --
        # resolve the cloned names from the clone's own index inventory.
        partials = await conn.fetch(
            """
            -- The WORKFLOW partial predicates (the clone's copied names
            -- drop the wf token; the PREDICATE is the identity).
            SELECT DISTINCT indexname
            FROM pg_indexes
            WHERE schemaname = $1
              AND tablename = $2
              AND (indexdef LIKE '%deps_pending > 0%'
                   OR indexdef LIKE '%parent_id IS NOT NULL%')
            """,
            schema,
            clone,
        )
        assert len(partials) >= 2, [dict(r) for r in partials]
        partial_sizes: dict[str, Any] = {}
        for rec in partials:
            idx = rec["indexname"]
            row = await conn.fetchrow(
                f"""
                SELECT
                    pg_relation_size('{schema}.{idx}') AS partial,
                    pg_relation_size('"{schema}".t03_full_control_idx') AS full
                """
            )
            assert row is not None
            full = row["full"] or 1
            ratio = (row["partial"] or 0) / full
            partial_sizes[idx] = {
                "partial_bytes": row["partial"],
                "full_bytes": row["full"],
                "ratio": ratio,
            }
            assert row["partial"] > 0, f"{idx} missing -- the clone did not build"
            assert ratio <= 0.01, (
                f"{idx} is {ratio:.4%} of the same-column full index -- "
                "the partial exemption is the doctrine"
            )
        partial_sizes["_red_drill"] = {
            "note": "the full control index against the smallest partial is the "
            "convicted shape; its ratio is the RED the pin exists to flag"
        }
        _write_measurement("pin2-partial-index.json", partial_sizes)
    finally:
        await conn.execute(f'DROP TABLE IF EXISTS "{width_schemas["extended"]}"."{clone}"')
        await conn.close()


def test_migrations_module_sees_the_round() -> None:
    keys = {m.key for m in migrate_mod.discover()}
    for seq in ("01", "04", "05", "06", "07"):
        assert f"{WORKFLOW_ROUND}_{seq}:pre" in keys, keys

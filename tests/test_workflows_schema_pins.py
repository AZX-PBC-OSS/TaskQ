"""T03's five §16.4 forever-pins + the migration round's structural pins.

The pins (ticket 03, RED-FIRST — every pin was collected RED then GREEN;
the red outputs live in ``.measurements/`` and the bands' artifact home is
``perf-evidence-workflows-schema.md``):

1. ROW-WIDTH — both table shapes built from the live migration files,
   10k seeded rows each: heap-bytes/row delta ≤ 8 AND avg-tuple delta ≤ 8
   (the measured residuals: +3.0 B tuple / 0.0 B heap — MAXALIGN padding
   absorbs the five NULL/0 columns). Red drill: a deliberately-misaligned
   padding fixture MUST blow the bound (the pin can fail).
2. PARTIAL-INDEX — each workflow partial index ≤ 1% of a same-column full
   index (the measured truth: 0.5%, 194x). Red drill: the FULL control
   index itself is the convicted fixture — the pin's ratio check flags it.
3. IMPORT — a fresh interpreter's ``import taskq`` leaves
   ``taskq.workflows`` absent from ``sys.modules`` (the §16.1 import law),
   implemented on the AST harness (``tests/_import_discipline.py``);
   red drill: the mutated-__init__ fixture is convicted by the same parser.
4. ENQUEUE-LATENCY NOISE BAND (``load_sensitive``) — vanilla enqueue p50
   inside the recorded band with the workflow-schema build present; the
   red drill touches the new columns on the hot path and must move the
   band (the pin can fail).
5. DISPATCH-CLAIM NOISE BAND (``load_sensitive``) — the claim statement's
   latency inside the dispatch evidence's band WITH the
   ``AND deps_pending = 0`` exclusion clause present (a semantic no-op for
   vanilla rows; the pin proves it stays that way); red drill: a fixture
   plan regression (index plans disabled) must blow the band.

Plus the structural pins the round inherits (the lock-scope/dead-index
family's form): the three-file single-lock-class split, the seam-only id
generation in the DDL (T04 pin 10's DDL half), the phase-obligations
header, and the additive-only rule (every file is a ``pre_`` file — no
workflow ``post_`` ever ships for v1).
"""

# Why: every f-string SQL below interpolates only this module's own throwaway schema identifiers (built from new_base62, validated by the migration runner's _IDENT_RE) or renders bundled migration files; all values are $n-bound.

from __future__ import annotations

import json
import subprocess
import sys
import time
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import asyncpg
import pytest

from taskq import migrate as migrate_mod
from taskq._ids import new_base62, new_job_id, new_uuid
from taskq.migrate import split_statements

WORKFLOW_ROUND = "01.00.23"
_MIGRATIONS_DIR = Path(__file__).parent.parent / "src" / "taskq" / "migrations"
_ROUND_FILES = sorted(_MIGRATIONS_DIR.glob(f"{WORKFLOW_ROUND}_*.sql"))
_MEASUREMENTS = Path(__file__).parent.parent / ".measurements"

_WF_COLUMNS = ("parent_id", "deps_pending", "map_index", "step_key", "code_version")


def _write_measurement(name: str, payload: Any) -> None:
    _MEASUREMENTS.mkdir(exist_ok=True)
    (_MEASUREMENTS / name).write_text(json.dumps(payload, indent=2, default=str))


# ── Structural pins: the single-lock-class split (family 1's form) ──────


def _round_files() -> list[Path]:
    assert _ROUND_FILES, f"the {WORKFLOW_ROUND} round is missing"
    return _ROUND_FILES


def test_round_is_split_into_single_lock_class_files() -> None:
    """The three files apply in order and each file is single-lock-class:
    the columns file ONLY metadata-only ALTERs, the tables file ONLY CREATE
    TABLEs, the index file ONLY CREATE INDEXes. The runner wraps each FILE
    in one transaction, so a file's statements share ONE write-block
    window — mixed lock classes in one file are the convicted shape (the
    lock-scope family's finding)."""
    names = [f.name for f in _round_files()]
    assert names == [
        "01.00.23_01_pre_workflow_columns.sql",
        "01.00.23_02_pre_workflow_tables.sql",
        "01.00.23_03_pre_workflow_indexes.sql",
    ], names


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
        while i < len(lines) and (
            not lines[i].strip() or lines[i].strip().startswith("--")
        ):
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
    sql = _round_files()[0].read_text()
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
    sql = _round_files()[1].read_text()
    kinds = _statement_kinds(sql)
    assert set(kinds) <= {"CREATE TABLE", "COMMENT ON"}, kinds
    for table in ("wf_edge", "wf_join_fire", "wf_outbox", "wf_step_ledger"):
        assert f'CREATE TABLE "{{schema}}".{table}' in sql, table


def test_indexes_file_holds_only_create_indexes() -> None:
    sql = _round_files()[2].read_text()
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


def test_import_law_red_drill() -> None:
    """The revert drill: the pin CAN fail — a module-level import of the
    workflows package in ``taskq/__init__.py`` is convicted by the same AST
    harness (the fixture mutation, parsed in isolation; the RED output is
    the offender list)."""
    import ast
    import types

    import taskq
    from tests import _import_discipline

    real_init = Path(taskq.__file__).read_text()
    mutated = "import taskq.workflows\n" + real_init
    fixture = types.ModuleType("taskq_fixture_mutated_init")
    fixture.__dict__["__file__"] = str(_MEASUREMENTS / "fixture_mutated_init.py")
    # Point the harness's source getter at the mutated text: the harness
    # parses module source, so the drill parses the MUTATED source and asks
    # the same question the pin asks.
    _MEASUREMENTS.mkdir(exist_ok=True)
    (Path(fixture.__dict__["__file__"])).write_text(mutated)
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

_SEED_ROWS = 10_000


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
        [
            (new_uuid(), "bench", "default", '{"pin": 1}', 3, "transient")
            for _ in range(rows)
        ],
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
            f'ALTER TABLE "{padded}".jobs ADD COLUMN IF NOT EXISTS {c} '
            f"{column_types[c]}"
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


async def _shape_stats(conn: asyncpg.Connection, target: str, rows: int) -> dict[str, float]:
    row = await conn.fetchrow(
        f"""
        SELECT
            pg_relation_size('"{target}".jobs')::float / {rows} AS heap_per_row,
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
    same-column FULL index (the measured truth: 0.5%, 194x). The full
    control index doubles as the pin's red drill — the ratio check flags
    exactly the full-index shape (recorded red)."""
    conn = await asyncpg.connect(width_schemas["dsn"])
    try:
        schema = width_schemas["extended"]
        # Scale to the evidence's shape (100k vanilla rows): the partial
        # index's fixed few-page overhead reads <1% only against a deep
        # table -- the measured truth (0.5%, 194x) is a 100k-row number.
        await conn.executemany(
            f'INSERT INTO "{schema}".jobs '
            "(id, actor, queue, payload, max_attempts, retry_kind) "
            "VALUES ($1, 'bench2', 'default', '{}', 3, 'transient')",
            [(new_uuid(),) for _ in range(100_000 - _SEED_ROWS)],
        )
        await conn.execute(
            f'CREATE INDEX IF NOT EXISTS t03_full_control_idx ON "{schema}".jobs (id)'
        )
        await conn.execute(f'ANALYZE "{schema}".jobs')
        partial_sizes: dict[str, Any] = {}
        for idx in (
            "jobs_wf_join_wait_idx",
            "jobs_wf_children_idx",
            "wf_outbox_undelivered_idx",
        ):
            row = await conn.fetchrow(
                f"""
                SELECT
                    pg_relation_size('"{schema}".{idx}') AS partial,
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
            assert row["partial"] > 0, f"{idx} missing — the index file did not apply"
            assert ratio <= 0.01, (
                f"{idx} is {ratio:.4%} of the same-column full index — "
                "the partial exemption is the doctrine"
            )
        partial_sizes["_red_drill"] = {
            "note": "the full control index against the smallest partial is the "
            "convicted shape; its ratio is the RED the pin exists to flag"
        }
        _write_measurement("pin2-partial-index.json", partial_sizes)
    finally:
        await conn.close()


# ── Pins 4/5: the noise bands (load_sensitive — the serial perf lane) ────

_PIN4_P50_BUDGET_US = 25_000


def _percentile(data: list[int], pct: float) -> int:
    ordered = sorted(data)
    idx = min(int(len(ordered) * pct / 100.0), len(ordered) - 1)
    return ordered[idx]


@pytest.mark.slow
@pytest.mark.integration
@pytest.mark.load_sensitive
async def test_pin_4_enqueue_latency_band(jobs_app: Any) -> None:
    """THE ENQUEUE-LATENCY NOISE BAND: vanilla enqueue p50 inside the
    recorded band with the workflow-schema build present (best-round p50 —
    the house benchmark's noise-robust statistic). RED DRILL (recorded):
    a fixture hook touching the new columns on the hot path (an extra
    round-trip read of the five columns per enqueue) must move the band —
    the pin can fail. Both measurements land in .measurements/."""
    from taskq.backend._protocol import EnqueueArgs
    from taskq.backend.postgres import PostgresBackend

    backend: PostgresBackend = jobs_app.backend

    async def measure_enqueue(extra_hook: bool) -> list[list[int]]:
        pool = backend._worker_pool  # benchmark-only: direct enqueue measurement
        schema = backend._schema_name  # benchmark-only
        rounds: list[list[int]] = []
        for _ in range(3):
            batch: list[int] = []
            for _ in range(50):
                start = time.perf_counter_ns()
                args = EnqueueArgs(
                    id=new_job_id(),
                    actor="bench",
                    queue="default",
                    payload={"pin": 4},
                    max_attempts=3,
                    retry_kind="transient",
                    scheduled_at=None,
                )
                await backend.enqueue(args)
                if extra_hook:
                    # THE RED-DRILL HOOK: the hot path touching the new
                    # columns — an extra round-trip read of all five, the
                    # cost-class regression the band must catch.
                    await pool.execute(
                        f"SELECT parent_id, deps_pending, map_index, step_key, "
                        f'code_version FROM "{schema}".jobs WHERE id = $1',
                        args.id,
                    )
                batch.append((time.perf_counter_ns() - start) // 1_000)
            rounds.append(batch)
        return rounds

    rounds: list[list[int]] = []
    rounds.extend(await measure_enqueue(extra_hook=False))
    best = min(rounds, key=sum)
    p50 = _percentile(best, 50)
    p99 = _percentile(best, 99)

    rounds.clear()
    rounds.extend(await measure_enqueue(extra_hook=True))
    red_best = min(rounds, key=sum)
    red_p50 = _percentile(red_best, 50)

    _write_measurement(
        "pin4-enqueue-band.json",
        {
            "pin": "enqueue-latency-band",
            "p50_us": p50,
            "p99_us": p99,
            "best_round_ms": best,
            "budget_us": _PIN4_P50_BUDGET_US,
            "red_drill": {
                "hook": "extra round-trip read of the five workflow columns per enqueue",
                "p50_us": red_p50,
                "best_round_ms": red_best,
                "moved_the_band": red_p50 > p50,
            },
        },
    )
    assert p50 <= _PIN4_P50_BUDGET_US, f"enqueue p50 {p50/1000:.2f} ms exceeds the band"
    assert red_p50 > p50, (
        "the red drill did not fire: the hot-path column touch did not move "
        "the band — the pin cannot fail, it is a decoration"
    )


@pytest.mark.slow
@pytest.mark.integration
@pytest.mark.load_sensitive
async def test_pin_5_dispatch_claim_band(jobs_app: Any) -> None:
    """THE DISPATCH-CLAIM NOISE BAND: the claim statement's best-round p50
    inside the band WITH the ``AND deps_pending = 0`` exclusion clause
    present (a semantic no-op for vanilla rows — the pin proves it stays
    that way). RED DRILL: a fixture plan regression (index plans disabled →
    Seq Scan) must blow the band — the pin can fail."""
    from datetime import UTC, datetime, timedelta

    from taskq._ids import new_uuid
    from taskq.backend._dispatch_sql import DISPATCH_STRICT_FIFO_SQL, dispatch_batch

    deps: Any = jobs_app.deps
    backend: Any = jobs_app.backend
    schema: str = deps.settings.schema_name
    pool = backend._dispatcher_pool  # benchmark-only: raw-CTE measurement

    worker_id = new_uuid()
    lock_lease = timedelta(seconds=90)
    queues = ["default"]
    now = datetime.now(tz=UTC)

    rows = [
        (
            new_uuid(),
            f"bench_{i % 10}",
            "default",
            '{"pin": 5}',
            3,
            "transient",
            "pending",
            now,
            i % 5,
            False,
            worker_id,
        )
        for i in range(1_000)
    ]
    await pool.executemany(
        f"""
        INSERT INTO "{schema}".jobs
            (id, actor, queue, payload, max_attempts, retry_kind, status, scheduled_at,
             priority, assignment_routed, locked_by_worker, lock_expires_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        """,
        [row + (now + lock_lease,) for row in rows],
    )
    await pool.execute(
        f'INSERT INTO "{schema}".actor_config (actor, queue, max_concurrent) '
        "SELECT 'bench_' || i, 'default', 10 FROM generate_series(0, 9) i "
        "ON CONFLICT (actor) DO NOTHING"
    )

    async def measure(rounds_n: int, per_round: int, **plan_gucs: str) -> list[int]:
        latencies: list[int] = []
        for _ in range(rounds_n):
            # Re-pend the round's rows: each dispatch claims (marks
            # running), so the backlog is reset between rounds — the
            # measured statement's cost stays the CLAIM's, not the drain's.
            await pool.execute(
                f"UPDATE \"{schema}\".jobs SET status = 'pending', "
                "locked_by_worker = NULL, lock_expires_at = NULL "
                "WHERE locked_by_worker = $1",
                worker_id,
            )
            batch: list[int] = []
            for _ in range(per_round):
                start = time.perf_counter_ns()
                async with pool.acquire() as conn, conn.transaction():
                    if plan_gucs:
                        await conn.execute(
                            " ".join(f"SET {k} = {v};" for k, v in plan_gucs.items())
                        )
                    await dispatch_batch(
                        conn,
                        sql=DISPATCH_STRICT_FIFO_SQL.format(schema=schema),
                        queues=queues,
                        limit_n=50,
                        worker_id=worker_id,
                        lock_lease=lock_lease,
                        oversample=2,
                    )
                batch.append((time.perf_counter_ns() - start) // 1_000)
            latencies.extend(batch)
        return latencies

    warm = await measure(1, 5)
    del warm
    samples = await measure(3, 20)
    p50 = _percentile(samples, 50)
    p99 = _percentile(samples, 99)

    # THE RED DRILL: the fixture plan regression — force the Seq-Scan plan
    # (the 83.7 ms monster class) and watch the band blow.
    red_samples = await measure(
        1,
        5,
        **{"enable_indexscan": "off", "enable_bitmapscan": "off", "enable_indexonlyscan": "off"},
    )
    red_p50 = _percentile(red_samples, 50)

    _write_measurement(
        "pin5-dispatch-band.json",
        {
            "pin": "dispatch-claim-band",
            "exclusion_clause": "AND deps_pending = 0",
            "p50_us": p50,
            "p99_us": p99,
            "budget_ms": 50,
            "samples": len(samples),
            "red_drill": {"plan": "seq-scan-forced", "p50_us": red_p50},
        },
    )
    assert p50 <= 50_000, (
        f"dispatch claim p50 {p50/1000:.2f} ms exceeds the band (the clause must stay free)"
    )
    assert red_p50 > p50, (
        "the red drill did not fire: the forced plan regression did not blow "
        "the band — the pin cannot fail, it is a decoration"
    )


def test_migrations_module_sees_the_round() -> None:
    keys = {m.key for m in migrate_mod.discover()}
    for seq in ("01", "02", "03"):
        assert f"{WORKFLOW_ROUND}_{seq}:pre" in keys, keys

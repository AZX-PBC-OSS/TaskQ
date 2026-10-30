"""THE FLOW-HISTORY RETENTION MIGRATION'S PINS (H-B5, H-B6) — the
01.00.35 index round's shape, structurally and on real PG.

H-B5 (the boot-time SHARE locks → the boot-race deadlock): the file
ORIGINALLY shipped in the runner's no-transaction +
``CREATE INDEX CONCURRENTLY`` shape and was REVERTED to the plain
transactional shape — the 01.00.06_01 law, re-proven by the
8-applier boot race (tests/test_attack_lifecycle_edges.py):
``apply_pending_locked`` serializes concurrent migrators with
``pg_advisory_lock``, a replica's blocking advisory wait is an open
transaction the CIC's blocker scan waits on, and the winner's CIC
cycles with the parked waiter — the deadlock detector fails the apply,
each new winner dies against the still-parked queue (7 of 8 lost in the
measured race), INVALID index debris left each time. A transactional
plain ``CREATE INDEX`` cannot cycle (its locks just queue behind the
advisory waiter) and rolls back whole on failure. The pins hold the
file to that law: NO no-transaction directive (``use_transaction is
True`` — the ledger records how the file RAN), no CONCURRENTLY
statement anywhere, the re-run clean, and the by-hand CONCURRENTLY note
for too-large deployments present (the maintenance-window escape that
keeps the huge-table SHARE-lock window off the boot).

H-B6 (the redundant index): ``wf_step_ledger_flow_idx`` is pure write
amplification on the family's hottest table — the claim arbiter
(``flow_id, step_key, COALESCE(map_index,-1), attempt``) already leads
with the flow probe. The file ships exactly TWO indexes (the edge and
join_fire flow indexes — the tables that had no flow_id-led index at
all), and the ledger's flow probe rides the arbiter's leading column.
"""

from __future__ import annotations

from pathlib import Path

import asyncpg
import pytest

from taskq import migrate as migrate_mod
from taskq.migrate import split_statements

MIGRATION_FILENAME = "01.00.35_01_pre_wf_flow_history_indexes.sql"
_MIGRATIONS_DIR = Path(__file__).parent.parent / "src" / "taskq" / "migrations"


def _migration_file() -> Path:
    path = _MIGRATIONS_DIR / MIGRATION_FILENAME
    assert path.exists(), path
    return path


def _migration() -> migrate_mod.Migration:
    discovered = [m for m in migrate_mod.discover() if m.filename == MIGRATION_FILENAME]
    assert len(discovered) == 1, discovered
    return discovered[0]


# ── H-B5: the transactional plain shape (the boot-race deadlock law) ─────


def test_migration_is_transactional_no_directive() -> None:
    """The file carries NO ``taskq:no-transaction`` directive and the
    runner applies it TRANSACTIONALLY: ``use_transaction is True`` — the
    no-transaction CONCURRENTLY form deadlocks the boot race
    (tests/test_attack_lifecycle_edges.py::test_boot_race_8_appliers_
    fresh_schema, the 01.00.06_01 law re-proven)."""
    sql = _migration_file().read_text()
    leading = []
    for line in sql.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("--"):
            leading.append(stripped)
            continue
        break
    assert not any("taskq:no-transaction" in line for line in leading), "\n".join(leading[:10])
    assert _migration().use_transaction is True, (
        "the runner would apply the file statement by statement outside a "
        "transaction — the CONCURRENTLY deadlock shape came back (the "
        "01.00.06_01 law; the boot race loses appliers to it)"
    )


def test_migration_statements_are_the_plain_shape() -> None:
    """Every statement body is ``DROP INDEX IF EXISTS`` /
    ``CREATE INDEX IF NOT EXISTS`` — NO ``CONCURRENTLY`` anywhere (the
    deadlock law: a CIC under apply_pending_locked's advisory lock
    cycles with the parked waiters; a plain build's locks just queue).
    Transactional + IF NOT EXISTS stays idempotent: a re-run skips the
    built indexes, and the DROP-first cures an INVALID HAND-built index
    (an interrupted by-hand CONCURRENTLY build's debris)."""
    sql = _migration_file().read_text()
    assert "CONCURRENTLY" not in "\n".join(
        line for line in sql.splitlines() if not line.strip().startswith("--")
    ), "a CONCURRENTLY statement rides outside the comments — the deadlock shape came back"
    bodies = [
        "\n".join(
            line for line in stmt.splitlines() if line.strip() and not line.strip().startswith("--")
        )
        for stmt in split_statements(sql)
    ]
    bodies = [b for b in bodies if b.strip()]
    assert bodies, "the migration is empty"
    for body in bodies:
        upper = " ".join(body.split()).upper()
        assert upper.startswith(("DROP INDEX", "CREATE INDEX")), body[:120]
    creates = [b for b in bodies if b.upper().startswith("CREATE")]
    assert len(creates) >= 2, creates
    # No transaction control (the runner would refuse; the pin states it).
    for body in bodies:
        assert not body.upper().startswith(("BEGIN", "COMMIT", "ROLLBACK")), body[:80]


def test_migration_ships_exactly_the_two_flow_indexes() -> None:
    """H-B6: the file builds wf_edge_flow_idx + wf_join_fire_flow_idx
    (the two tables with no flow_id-led index) and does NOT build
    ``wf_step_ledger_flow_idx`` — the claim arbiter's leading column
    serves the flow probe; the redundant index was pure write
    amplification on the hottest table."""
    sql = _migration_file().read_text()
    assert "wf_edge_flow_idx" in sql
    assert "wf_join_fire_flow_idx" in sql
    assert "wf_step_ledger_flow_idx" not in sql, (
        "wf_step_ledger_flow_idx is REDUNDANT (the claim arbiter's leading "
        "flow_id column serves the probe) — pure write amplification (H-B6)"
    )
    # The by-hand note for the TOO-LARGE deployments stays.
    assert "maintenance window" in sql.lower() or "by hand" in sql.lower(), (
        "the ops note for too-large deployments is gone"
    )


def test_migration_ops_note_carries_the_measured_lock_threshold() -> None:
    """R5: the ops note's too-large advice was UNMEASURED — an operator
    had no number to threshold the by-hand build on. The note now carries
    the MEASURED figure (~1.0 s @ 3M rows on warm NVMe — the round-2's
    data; the boot's SHARE lock blocks ALL writes to the table for that
    span), states the cloud-volume multiple honestly (a multiple of the
    local number, measure yours), and gives the by-hand threshold's
    SHAPE: beyond ~1M rows on cloud volumes, build by hand in a
    maintenance window."""
    sql = _migration_file().read_text()
    lowered = sql.lower()
    ops_at = lowered.find("ops note (locks)")
    assert ops_at >= 0, "the OPS NOTE (locks) section is gone"
    note = lowered[ops_at:]
    for token in (
        "measured",  # the figure is stated as a measurement, not a guess
        "3m",  # the measured scale: ~1.0 s @ 3M rows
        "1.0",  # the measured span
        "share lock",  # the lock class the boot takes
        "blocks all writes",  # what the SHARE lock does for the build's span
        "cloud",  # the honest multiple: cloud volumes are slower, measure yours
        "~1m",  # the by-hand threshold's shape
        "maintenance window",  # the by-hand build's venue
    ):
        assert token in note, f"the ops note lost its measured threshold: {token!r} missing"


# ── The runner acceptance, on real PG ────────────────────────────────────


async def test_migration_ran_and_reruns_clean(wf_conn: asyncpg.Connection, wf_schema: str) -> None:
    """The module schema's own ledger is the runner's ACCEPTANCE receipt:
    the file applied through the real runner (the fixtures apply the
    bundled set) with ``use_transaction = true`` recorded — the
    transactional shape the boot-race deadlock law demands. The file then
    RE-RUNS clean on the live schema (every statement re-executes
    idempotently: IF NOT EXISTS skips the built indexes), and the
    indexes are VALID with the redundant one absent."""
    # Why: the f-string interpolates only the fixture's settings-validated schema identifier (the runner's _IDENT_RE); the version key is the module's own constant.
    row = await wf_conn.fetchrow(
        f'SELECT use_transaction FROM "{wf_schema}".schema_migrations '  # noqa: S608
        "WHERE version = $1",
        "01.00.35_01:pre",
    )
    assert row is not None, "the migration is not in the module schema's ledger"
    assert row["use_transaction"] is True, (
        "the ledger records the file as non-transactional — the "
        "no-transaction CONCURRENTLY shape came back (the boot-race "
        "deadlock law)"
    )

    # The re-run: every statement again (the file is idempotent by
    # construction — IF NOT EXISTS + DROP IF EXISTS).
    migration = _migration()
    rendered = migration.render(wf_schema)
    statements = split_statements(rendered)
    assert len(statements) >= 4  # two DROP + two CREATE
    for statement in statements:
        await wf_conn.execute(statement)

    indexes = {
        rec["indexname"]: rec["indisvalid"]
        for rec in await wf_conn.fetch(
            """
            SELECT i.relname AS indexname, idx.indisvalid
            FROM pg_index idx
            JOIN pg_class i ON i.oid = idx.indexrelid
            JOIN pg_class t ON t.oid = idx.indrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            WHERE n.nspname = $1
              AND t.relname IN ('wf_edge', 'wf_join_fire', 'wf_step_ledger')
            """,
            wf_schema,
        )
    }
    assert indexes.get("wf_edge_flow_idx") is True, indexes
    assert indexes.get("wf_join_fire_flow_idx") is True, indexes
    assert "wf_step_ledger_flow_idx" not in indexes, indexes


@pytest.mark.parametrize(
    ("table", "index_name"),
    [("wf_edge", "wf_edge_flow_idx"), ("wf_join_fire", "wf_join_fire_flow_idx")],
)
async def test_flow_index_serves_the_flow_probe(
    wf_conn: asyncpg.Connection, wf_schema: str, table: str, index_name: str
) -> None:
    """The shipped indexes serve the arms' delete join leg: a per-flow
    probe on either table plans against the flow index (the two-pass
    law's delete half rides it)."""
    # Why: the f-string interpolates only the fixture's settings-validated schema identifier and the pin module's own parametrized table name (never user input); the probe's value is a bound parameter.
    plan = await wf_conn.fetchval(
        f'EXPLAIN (FORMAT JSON) SELECT 1 FROM "{wf_schema}".{table} '  # noqa: S608
        "WHERE flow_id = $1::uuid",
        "00000000-0000-0000-0000-000000000000",
    )
    plan_text = str(plan)
    assert index_name in plan_text, (
        f"the flow probe on {table} does not plan against {index_name}:\n{plan_text[:400]}"
    )

"""The assignment-routed marker round's lock-scope split (#250).

Two pin families:

1. Structural (no PG) — the restructured round is split into
   single-purpose files so no transaction mixes lock classes on ``jobs``:
   the columns file holds ONLY the two metadata-only ALTERs (whose ACCESS
   EXCLUSIVE is held for milliseconds), the backfill file ONLY the
   bounded UPDATE (ROW EXCLUSIVE — blocks neither readers nor writers),
   the index file ONLY its CREATE INDEX statements (SHARE, one build per
   transaction, so the writes queued behind one build drain before the
   next asks for the table). The original single-transaction form held
   ACCESS EXCLUSIVE across the backfill and both full-table index builds
   (#250).

2. Schema equivalence (PG) — the consumer upgrade path (a ledger at the
   pinned-consumer baseline — the bundled set ends at 01.00.05_01 —
   applying every pending migration, pre phase then post, exactly the
   gated one-shot ``apply_pending_locked`` deploy shape) reaches the
   SAME final ``jobs``/``jobs_archive`` inventory as a fresh full apply:
   identical index definitions, identical columns, identical
   constraints, identical ledger keys. The end state itself is pinned by
   name so the restructure cannot silently drop or reshape a structure
   the frozen files below it declare.
"""

from __future__ import annotations

import asyncpg
import pytest

from taskq import migrate as migrate_mod
from taskq._ids import new_base62
from taskq.migrate import split_statements

# ── 1. Structural pins: the split ───────────────────────────────────────────

_MARKER_ROUND_KEYS = (
    "01.00.12_05:pre",  # columns only
    "01.00.12_07:pre",  # backfill only
    "01.00.12_08:pre",  # assignment-routed probe indexes only
)


def _rendered(filename: str) -> str:
    by_name = {m.filename: m for m in migrate_mod.discover()}
    migration = by_name[filename]
    return migration.render("taskq")


def _statement_bodies(sql: str) -> list[str]:
    """Each statement's SQL body, leading ``--`` comments stripped.

    split_statements leaves a statement's leading comments attached; the
    migration files under test use ``--`` comments only.
    """
    bodies: list[str] = []
    for stmt in split_statements(sql):
        lines = stmt.splitlines()
        i = 0
        while i < len(lines) and lines[i].strip().startswith("--"):
            i += 1
        bodies.append("\n".join(lines[i:]))
    return bodies


def _statement_kinds(sql: str) -> list[str]:
    """Classify each statement's leading keyword shape: ALTER TABLE,
    UPDATE, CREATE INDEX, CREATE UNIQUE INDEX — enough for the lock-class
    pins without a SQL parser."""
    kinds: list[str] = []
    for body in _statement_bodies(sql):
        collapsed = " ".join(body.split()).upper().replace('"', "")
        if collapsed.startswith("CREATE UNIQUE INDEX"):
            kinds.append("CREATE UNIQUE INDEX")
        elif collapsed.startswith("CREATE INDEX"):
            kinds.append("CREATE INDEX")
        elif collapsed.startswith("ALTER TABLE"):
            kinds.append("ALTER TABLE")
        elif collapsed.startswith("UPDATE"):
            kinds.append("UPDATE")
        else:
            kinds.append(collapsed.split(" ")[0])
    return kinds


def test_marker_round_is_split_into_single_purpose_files_in_order() -> None:
    """The marker round's files apply in dependency order (columns before
    the backfill that writes the column, backfill before the probe
    indexes that read the flag), with the pre-existing 01.00.12_06 tag
    indexes untouched between them."""
    keys = [m.key for m in migrate_mod.discover()]
    positions = [keys.index(k) for k in _MARKER_ROUND_KEYS]
    assert positions == sorted(positions), (
        f"marker-round files out of order: {keys}"
    )
    tag_idx = keys.index("01.00.12_06:pre")
    assert positions[0] < tag_idx < positions[1], (
        "the pre-existing 01.00.12_06 must stay between the columns file "
        "and the backfill (renumbering it was deliberately avoided)"
    )


def test_columns_file_holds_only_the_two_metadata_alters() -> None:
    """The ACCESS EXCLUSIVE window is exactly the two catalog writes plus
    the commit: nothing table-sized may share the columns file's
    transaction (#250)."""
    sql = _rendered("01.00.12_05_pre_assignment_routed_columns.sql")
    kinds = _statement_kinds(sql)
    assert kinds == ["ALTER TABLE", "ALTER TABLE"], kinds
    bodies = _statement_bodies(sql)
    assert all("ADD COLUMN IF NOT EXISTS assignment_routed" in b for b in bodies), bodies
    assert any('"taskq".jobs' in b for b in bodies) and any(
        '"taskq".jobs_archive' in b for b in bodies
    ), bodies
    assert not any("UPDATE" in b.upper() or "CREATE" in b.upper() for b in bodies), bodies


def test_backfill_file_holds_only_the_bounded_update() -> None:
    """The backfill runs under ROW EXCLUSIVE (it blocks neither readers
    nor other writers), which is only true if it does not share a
    transaction with the columns' ALTER — and its population stays
    EXACTLY the pre-upgrade proxy's: dispatchable-now or promotable rows
    claimed at least once, never the whole table."""
    sql = _rendered("01.00.12_07_pre_assignment_routed_backfill.sql")
    kinds = _statement_kinds(sql)
    assert kinds == ["UPDATE"], kinds
    bodies = _statement_bodies(sql)
    (body,) = bodies
    assert "SET assignment_routed = true" in body
    assert "status IN ('pending', 'scheduled')" in body
    assert "started_at IS NOT NULL" in body
    assert "assignment_routed = false" in body
    assert "ALTER TABLE" not in body.upper() and "CREATE" not in body.upper()


def test_probe_index_file_holds_only_its_builds() -> None:
    """Each index build is its own transaction's only table-sized work, so
    the writes queued behind one build drain before the next asks for the
    table (#250). The end-state definitions are byte-preserved from the
    original single-file round."""
    sql = _rendered("01.00.12_08_pre_assignment_routed_probe_indexes.sql")
    assert _statement_kinds(sql) == ["CREATE INDEX", "CREATE INDEX"]
    bodies = _statement_bodies(sql)
    assert not any("ALTER TABLE" in b.upper() or "UPDATE" in b.upper() for b in bodies)
    body_sql = "\n".join(bodies)
    assert "jobs_assignment_routed_probe_idx" in body_sql
    assert "jobs_actor_queue_backlog_idx" in body_sql
    assert "WHERE status = 'pending' AND assignment_routed" in body_sql
    assert "WHERE status IN ('pending', 'scheduled')" in body_sql


def test_marker_round_files_stay_transactional() -> None:
    """The split must not drift into the no-transaction CONCURRENTLY form:
    the runner's advisory-lock deadlock doctrine (01.00.09_01's
    derivation) keeps every bundled index build a transactional plain
    CREATE INDEX, and per-file rollback is what makes the split's
    re-run-on-failure story safe."""
    by_key = {m.key: m for m in migrate_mod.discover()}
    for key in _MARKER_ROUND_KEYS:
        assert by_key[key].use_transaction, key


# ── 2. Schema equivalence: consumer path vs fresh install ──────────────────

# The pinned end-state index inventory of ``jobs``: every index the frozen
# and unchanged migration files below the marker round declare, plus the
# marker round's own two. Derived from the files, not from a live apply,
# so agreement between this list and the database is the proof the
# restructure changed only structure.
_PINNED_JOBS_INDEXES: frozenset[str] = frozenset({
    # 01.00.00_01 (jobs_idempotency_key_uniq is dropped by 01.00.03_01:post,
    # jobs_actor_fairness_dispatch_idx by 01.00.09_01:post)
    "jobs_pkey",
    "jobs_dispatch_idx",
    "jobs_actor_dispatch_idx",
    "jobs_scheduled_wake_idx",
    "jobs_running_lock_expires_idx",
    "jobs_schedule_to_close_idx",
    "jobs_identity_active_idx",
    "jobs_singleton_uniq",
    "jobs_actor_running_idx",
    "jobs_actor_pending_idx",
    "jobs_finished_at_idx",
    "jobs_metadata_gin_idx",
    "jobs_cancel_requested_idx",
    "jobs_locked_by_worker_running_idx",
    "jobs_result_expires_at_idx",
    "jobs_tags_gin_idx",
    # 01.00.03_01:pre
    "jobs_idempotency_scope_key_uniq",
    # 01.00.06_01
    "jobs_queue_active_idx",
    "jobs_actor_active_id_idx",
    # 01.00.09_01
    "jobs_round_robin_probe_idx",
    # 01.00.10_01
    "jobs_running_heartbeat_deadline_idx",
    # 01.00.12_06
    "jobs_tags_active_gin_idx",
    "jobs_tags_cancellable_running_gin_idx",
    # 01.00.12_08 (this round: assignment-routed population)
    "jobs_assignment_routed_probe_idx",
    "jobs_actor_queue_backlog_idx",
    # 01.00.13_02
    "jobs_queue_actor_dispatch_idx",
    # 01.00.13_03
    "jobs_batch_open_members_idx",
})
_PINNED_JOBS_ARCHIVE_INDEXES: frozenset[str] = frozenset({
    "jobs_archive_pkey",
    "jobs_archive_expire_at_idx",
    "jobs_archive_finished_at_idx",
    "jobs_archive_tags_gin_idx",
})

# The bundled set of the pinned-consumer baseline (bd30c1b): everything
# through 01.00.05_01 is already in the upgrading fleet's ledger, and the
# rest is what the one-shot migrate applies.
_CONSUMER_BASELINE_TARGET = "01.00.05_01"


async def _drop_schema(conn: asyncpg.Connection, schema: str) -> None:
    await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


async def _index_inventory(conn: asyncpg.Connection, schema: str, table: str) -> dict[str, str]:
    """{index name: pg_get_indexdef} for every index on *table*.

    The schema's own name is normalized out of the rendered definition
    (pg_get_indexdef qualifies enum literals with it, e.g.
    ``'pending'::{schema}.job_status``), so two schemas' inventories are
    comparable.
    """
    rows = await conn.fetch(
        """
        SELECT c.relname, pg_get_indexdef(c.oid) AS def
        FROM pg_index i
        JOIN pg_class c ON c.oid = i.indexrelid
        JOIN pg_class t ON t.oid = i.indrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = $1 AND t.relname = $2
        ORDER BY c.relname
        """,
        schema,
        table,
    )
    return {r["relname"]: str(r["def"]).replace(f"{schema}.", "<schema>.") for r in rows}


async def _column_names(conn: asyncpg.Connection, schema: str, table: str) -> list[str]:
    rows = await conn.fetch(
        """
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = $1 AND table_name = $2
        ORDER BY ordinal_position
        """,
        schema,
        table,
    )
    return [str(r["column_name"]) for r in rows]


async def _check_constraints(conn: asyncpg.Connection, schema: str, table: str) -> list[str]:
    rows = await conn.fetch(
        """
        SELECT con.conname, pg_get_constraintdef(con.oid) AS def
        FROM pg_constraint con
        JOIN pg_class t ON t.oid = con.conrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = $1 AND t.relname = $2
        ORDER BY con.conname
        """,
        schema,
        table,
    )
    return [
        f"{r['conname']}: {r['def']!s}".replace(f"{schema}.", "<schema>.")
        for r in rows
    ]


@pytest.mark.integration
async def test_consumer_upgrade_path_reaches_the_pinned_end_state(pg_dsn: str) -> None:
    """A ledger at the pinned-consumer baseline, upgraded by the one-shot
    gated deploy shape (pre phase, then post), ends on EXACTLY the fresh
    install's schema: same index definitions, columns, constraints and
    ledger keys — the restructure is invisible to taskq_migrate state and
    to any fresh install (#250's red-team: restructuring must be
    end-state-preserving)."""
    fresh = f"mig_eq_fresh_{new_base62()}".lower()
    upgraded = f"mig_eq_up_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await _drop_schema(conn, fresh)
        await _drop_schema(conn, upgraded)

        # Path A: fresh install, one full apply.
        await migrate_mod.apply_pending(conn, schema=fresh)

        # Path B: the consumer path — the baseline ledger first...
        baseline = await migrate_mod.apply_pending(
            conn, schema=upgraded, target=_CONSUMER_BASELINE_TARGET
        )
        assert "01.00.03_01:post" in [m.key for m in baseline], (
            "the baseline must include the released post-phase drop, as a "
            "long-running fleet's ledger does"
        )
        # ...then the gated one-shot upgrade: pre phase, then post.
        await migrate_mod.apply_pending(conn, schema=upgraded, phase="pre")
        await migrate_mod.apply_pending(conn, schema=upgraded, phase="post")

        for table in ("jobs", "jobs_archive"):
            inv_a = await _index_inventory(conn, fresh, table)
            inv_b = await _index_inventory(conn, upgraded, table)
            assert inv_a == inv_b, (
                f"{table}: the consumer upgrade path diverged from a fresh "
                f"install:\nfresh only: {set(inv_a) - set(inv_b)}\n"
                f"upgraded only: {set(inv_b) - set(inv_a)}\n"
                + "\n".join(
                    f"{k}:\n  fresh:     {inv_a[k]}\n  upgraded: {inv_b[k]}"
                    for k in sorted(set(inv_a) & set(inv_b))
                    if inv_a[k] != inv_b[k]
                )
            )
            cols_a = await _column_names(conn, fresh, table)
            cols_b = await _column_names(conn, upgraded, table)
            assert cols_a == cols_b, f"{table} columns diverged: {cols_a} vs {cols_b}"
            cons_a = await _check_constraints(conn, fresh, table)
            cons_b = await _check_constraints(conn, upgraded, table)
            assert cons_a == cons_b, f"{table} constraints diverged"

        # The end state is pinned by name: the restructured round's own
        # indexes are here and nothing the older files declare went
        # missing on either path.
        for schema, pinned in (
            (fresh, _PINNED_JOBS_INDEXES),
            (upgraded, _PINNED_JOBS_INDEXES),
        ):
            names = set(await _index_inventory(conn, schema, "jobs"))
            assert names == pinned, (
                f"jobs index inventory drifted from the pinned end state "
                f"(missing: {pinned - names}, unexpected: {names - pinned})"
            )
        for schema in (fresh, upgraded):
            archive_names = set(await _index_inventory(conn, schema, "jobs_archive"))
            assert archive_names == _PINNED_JOBS_ARCHIVE_INDEXES, archive_names

        # Every index is VALID (no interrupted-build debris).
        assert await migrate_mod.list_invalid_indexes(conn, fresh) == []
        assert await migrate_mod.list_invalid_indexes(conn, upgraded) == []

        # Ledger identity: both paths record the same full key set,
        # including every restructured file.
        ledger_a = set(await migrate_mod.list_applied(conn, fresh))
        ledger_b = set(await migrate_mod.list_applied(conn, upgraded))
        assert ledger_a == ledger_b, (
            f"ledger keys diverged: {ledger_a ^ ledger_b}"
        )
        assert all(key in ledger_b for key in _MARKER_ROUND_KEYS)
    finally:
        await _drop_schema(conn, fresh)
        await _drop_schema(conn, upgraded)
        await conn.close()

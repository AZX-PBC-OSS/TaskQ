"""The assignment-routed marker round's lock-scope split (#250), the dead
probe index pair's removal (#243), and the unrouted probe indexes (#243).

Three pin families:

1. Structural (no PG): the restructured round is split into
   single-purpose files so no transaction mixes lock classes on ``jobs``:
   the columns file holds ONLY the two metadata-only ALTERs (whose ACCESS
   EXCLUSIVE is held for milliseconds), the backfill file ONLY the
   bounded UPDATE (ROW EXCLUSIVE: blocks neither readers nor writers),
   the index files ONLY their CREATE INDEX statements (SHARE). The
   runner wraps each FILE, not each statement, in one transaction, so
   a file's builds share ONE write-block window (its duration is the
   sum of the file's builds, with no drain between builds inside a
   file) and the writes queued behind it drain only between FILES.
   What the split bounds is the window per file plus the lock MODE:
   the original single-file form held ACCESS EXCLUSIVE, which blocks
   reads too, across the ALTERs, the backfill and both builds in ONE
   window (#250). The dead pair (01.00.11_01's create +
   01.00.12_05:post's drop of ``jobs_repended_probe_idx``, referenced by
   no code at HEAD) is gone from the bundled set entirely (#243).

2. Schema equivalence (PG): the consumer upgrade path (a ledger at the
   pinned-consumer baseline, the bundled set ends at 01.00.05_01,
   applying every pending migration, pre phase then post, exactly the
   gated one-shot ``apply_pending_locked`` deploy shape) reaches the
   SAME final ``jobs``/``jobs_archive`` inventory as a fresh full apply:
   identical index definitions, identical columns, identical
   constraints, identical ledger keys. The end state itself is pinned by
   name so the restructure cannot silently drop or reshape a structure
   the frozen files below it declare.

3. Plan oracles (PG): the #243 measurement shape (re-pended rows ahead
   of producer-placed rows on the same (actor, queue)): the production
   candidates laterals' probes ride the new marker-partial indexes,
   ``NOT assignment_routed`` is served by the index predicate instead of
   walking the re-pended tail as a post-scan Filter.
"""

# ruff: noqa: S608  # Why: every f-string SQL below interpolates only this module's own throwaway schema identifier (built from new_base62, validated by the migration runner's _IDENT_RE) or renders a module SQL constant; all values are $n-bound.

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg
import pytest

from taskq import migrate as migrate_mod
from taskq._ids import new_base62, new_uuid
from taskq.backend._dispatch_sql import (
    _ROUND_ROBIN_CANDIDATES_LATERAL,  # pyright: ignore[reportPrivateUsage]  # Why: pinning the production lateral, not a copy; a copy could drift from the SQL that actually runs.
    _STRICT_FIFO_CANDIDATES_LATERAL,  # pyright: ignore[reportPrivateUsage]  # Why: same as above.
    DISPATCH_ROUND_ROBIN_SQL,
    DISPATCH_STRICT_FIFO_SQL,
)
from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: reusing the canonical identifier regex rather than redefining it.
)
from taskq.migrate import split_statements

# ── 1. Structural pins: the split, and the dead pair's removal ─────────────

_MARKER_ROUND_KEYS = (
    "01.00.12_05:pre",  # columns only
    "01.00.12_07:pre",  # backfill only
    "01.00.12_08:pre",  # assignment-routed probe indexes only
    "01.00.12_09:pre",  # unrouted probe indexes only
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
    UPDATE, CREATE INDEX, CREATE UNIQUE INDEX, enough for the lock-class
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


def test_dead_probe_index_pair_is_gone_from_the_bundled_set() -> None:
    """01.00.11_01's create and 01.00.12_05:post's drop of
    jobs_repended_probe_idx are removed: no code at HEAD reads that
    predicate, so the pair only made every upgrade pay one write-blocking
    full-table build for an index nothing uses (#243)."""
    keys = {m.key for m in migrate_mod.discover()}
    assert "01.00.11_01:pre" not in keys
    assert "01.00.12_05:post" not in keys, (
        "the drop file went with the create file: keeping the post alone "
        "would fail every apply with a DROP of an index that no longer exists"
    )
    filenames = {m.filename for m in migrate_mod.discover()}
    assert not any("repended" in name for name in filenames), filenames


def test_marker_round_is_split_into_single_purpose_files_in_order() -> None:
    """The marker round's four files apply in dependency order (columns
    before the backfill that writes the column, backfill before the
    probe indexes that read the flag), with the pre-existing
    01.00.12_06 tag indexes untouched between them."""
    keys = [m.key for m in migrate_mod.discover()]
    positions = [keys.index(k) for k in _MARKER_ROUND_KEYS]
    assert positions == sorted(positions), f"marker-round files out of order: {keys}"
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
    transaction with the columns' ALTER, and its population stays
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


def test_probe_index_files_hold_only_their_builds() -> None:
    """A file's builds share its ONE transaction (the runner wraps each
    FILE, not each statement: the doctrine 01.00.12_06's own header
    states), so the write-block window is the file's builds summed and
    drains only between files; what the split bounds is the window per
    FILE and the lock MODE (SHARE blocks writes only; the original
    ACCESS EXCLUSIVE blocked reads too) (#250). The end-state
    definitions are byte-preserved from the original single-file round."""
    routed = _rendered("01.00.12_08_pre_assignment_routed_probe_indexes.sql")
    assert _statement_kinds(routed) == ["CREATE INDEX", "CREATE INDEX"]
    routed_bodies = _statement_bodies(routed)
    assert not any("ALTER TABLE" in b.upper() or "UPDATE" in b.upper() for b in routed_bodies)
    routed_sql = "\n".join(routed_bodies)
    assert "jobs_assignment_routed_probe_idx" in routed_sql
    assert "jobs_actor_queue_backlog_idx" in routed_sql
    assert "WHERE status = 'pending' AND assignment_routed" in routed_sql
    assert "WHERE status IN ('pending', 'scheduled')" in routed_sql

    unrouted = _rendered("01.00.12_09_pre_unrouted_dispatch_probe_indexes.sql")
    assert _statement_kinds(unrouted) == ["CREATE INDEX", "CREATE INDEX"]
    unrouted_bodies = _statement_bodies(unrouted)
    assert not any("ALTER TABLE" in b.upper() or "UPDATE" in b.upper() for b in unrouted_bodies)
    unrouted_sql = "\n".join(unrouted_bodies)
    assert "jobs_unrouted_actor_dispatch_idx" in unrouted_sql
    assert "jobs_unrouted_round_robin_probe_idx" in unrouted_sql
    assert unrouted_sql.count("WHERE status = 'pending' AND NOT assignment_routed") == 2


def test_unrouted_index_predicates_match_the_dispatch_arms_exactly() -> None:
    """A partial index serves a query only when the planner can prove the
    query's quals imply the index predicate, and the probe must not
    reorder around the marker: the arms' conjuncts and the indexes'
    predicates must stay the same population, VERBATIM (#243's
    coordination requirement, the predicates match the dispatch SQL as
    it exists, sibling worktrees' semantics changes renumber here)."""
    for lateral in (_STRICT_FIFO_CANDIDATES_LATERAL, _ROUND_ROBIN_CANDIDATES_LATERAL):
        assert "AND NOT j2.assignment_routed" in lateral
        assert "AND j2.status = 'pending'" in lateral
    unrouted = _rendered("01.00.12_09_pre_unrouted_dispatch_probe_indexes.sql")
    # The round-robin twin's COALESCE expression must be verbatim-identical
    # to the lateral's probe equality, or the expression index stops
    # serving the query.
    assert "COALESCE(fairness_key, '__null__')" in unrouted
    assert "COALESCE(j2.fairness_key, '__null__') = k.fkey" in _ROUND_ROBIN_CANDIDATES_LATERAL


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
# marker round's own four (12_08/12_09), plus the primary key. Derived
# from the files, not from a live apply, so agreement between this list
# and the database is the proof the restructure changed only structure.
_PINNED_JOBS_INDEXES: frozenset[str] = frozenset(
    {
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
        # 01.00.12_09 (this round: producer-placed population)
        "jobs_unrouted_actor_dispatch_idx",
        "jobs_unrouted_round_robin_probe_idx",
        # 01.00.13_02
        "jobs_queue_actor_dispatch_idx",
        # 01.00.13_03
        "jobs_batch_open_members_idx",
    }
)
_PINNED_JOBS_ARCHIVE_INDEXES: frozenset[str] = frozenset(
    {
        "jobs_archive_pkey",
        "jobs_archive_expire_at_idx",
        "jobs_archive_finished_at_idx",
        "jobs_archive_tags_gin_idx",
    }
)

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
    return [f"{r['conname']}: {r['def']!s}".replace(f"{schema}.", "<schema>.") for r in rows]


@pytest.mark.integration
async def test_consumer_upgrade_path_reaches_the_pinned_end_state(pg_dsn: str) -> None:
    """A ledger at the pinned-consumer baseline, upgraded by the one-shot
    gated deploy shape (pre phase, then post), ends on EXACTLY the fresh
    install's schema: same index definitions, columns, constraints and
    ledger keys, the restructure is invisible to taskq_migrate state and
    to any fresh install (#250's adversarial review: restructuring must be
    end-state-preserving; #243's dead pair and new indexes are the only
    sanctioned deltas, both pinned by name here)."""
    fresh = f"mig_eq_fresh_{new_base62()}".lower()
    upgraded = f"mig_eq_up_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await _drop_schema(conn, fresh)
        await _drop_schema(conn, upgraded)

        # Path A: fresh install, one full apply.
        await migrate_mod.apply_pending(conn, schema=fresh)

        # Path B: the consumer path, the baseline ledger first...
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
        # indexes are here, the dead probe index is not, and nothing the
        # older files declare went missing on either path.
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

        # Every index is VALID (no interrupted-build debris) and the dead
        # probe index is absent by name on both paths.
        assert await migrate_mod.list_invalid_indexes(conn, fresh) == []
        assert await migrate_mod.list_invalid_indexes(conn, upgraded) == []
        for schema in (fresh, upgraded):
            inv = await _index_inventory(conn, schema, "jobs")
            assert "jobs_repended_probe_idx" not in inv
            for name in ("jobs_unrouted_actor_dispatch_idx", "jobs_unrouted_round_robin_probe_idx"):
                assert "NOT assignment_routed" in inv[name], (
                    f"{schema}.{name} lost its marker predicate: {inv[name]}"
                )

        # Ledger identity: both paths record the same full key set, and
        # the dead pair's keys appear on neither.
        ledger_a = set(await migrate_mod.list_applied(conn, fresh))
        ledger_b = set(await migrate_mod.list_applied(conn, upgraded))
        assert ledger_a == ledger_b, f"ledger keys diverged: {ledger_a ^ ledger_b}"
        assert "01.00.11_01:pre" not in ledger_b
        assert all(key in ledger_b for key in _MARKER_ROUND_KEYS)
    finally:
        await _drop_schema(conn, fresh)
        await _drop_schema(conn, upgraded)
        await conn.close()


# ── 3. Plan oracles: the label-routed probes never walk re-pended rows ─────

# The #243 measurement shape: re-pended pending rows AHEAD of the
# producer-placed rows in the probes' (priority DESC, ...) order on the
# same (actor, queue), so the pending-only twins' ordered scans must walk
# them all before reaching an admissible row.
_REPEND_ROWS = 20_000
_PRODUCER_ROWS = 2_000


@pytest.fixture(scope="module")
async def mixed_population_schema(pg_dsn: str) -> Any:
    """Throwaway schema, migrations applied, bulk-seeded with the mixed
    population, then ANALYZEd so the planner's partial-index size
    estimates reflect the 10:1 re-pend skew."""
    schema = f"unrouted_oracle_{new_base62()}".lower()
    assert _IDENT_RE.match(schema)
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await migrate_mod.apply_pending(conn, schema=schema)
        await conn.execute(
            f'INSERT INTO "{schema}".actor_config (actor, queue) '
            "VALUES ('unrouted_probe', 'default')"
        )
        now = datetime.now(UTC)

        def _rows(count: int, routed: bool, priority: int) -> list[tuple[Any, ...]]:
            return [
                (
                    new_uuid(),
                    "unrouted_probe",
                    "default",
                    '{"v": 1}',
                    "pending",
                    priority,
                    now - timedelta(minutes=1),
                    3,
                    "transient",
                    routed,
                )
                for _ in range(count)
            ]

        columns = [
            "id",
            "actor",
            "queue",
            "payload",
            "status",
            "priority",
            "scheduled_at",
            "max_attempts",
            "retry_kind",
            "assignment_routed",
        ]
        await conn.copy_records_to_table(
            "jobs",
            schema_name=schema,
            columns=columns,
            records=_rows(_REPEND_ROWS, True, 100),
        )
        await conn.copy_records_to_table(
            "jobs",
            schema_name=schema,
            columns=columns,
            records=_rows(_PRODUCER_ROWS, False, 1),
        )
        await conn.execute(f'ANALYZE "{schema}".jobs')
        yield schema
    finally:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


def _params_wrapper(lateral: str, schema: str, *, rr_keys: bool) -> str:
    """Wrap a production candidates lateral with literalized outer
    producers (the same doctrine as tests/test_sweepaudit_dispatch_bound.py):
    only the outer CTE names the lateral references are replaced, the
    lateral itself is rendered verbatim, and the five typed parameters are
    bound exactly as the real statement binds them. ``rr_keys`` (the
    round-robin lateral's cohort enumeration, which the recursive original
    cannot be correlated) is literalized to the one seeded cohort."""
    ctes = [
        "params AS (SELECT $1::text[] AS queues, $2::int AS limit_n, "
        "$3::uuid AS worker_id, $4::interval AS lock_lease, $5::int AS oversample)"
    ]
    if rr_keys:
        ctes.append(
            "rr_keys (actor, queue, fkey) AS (VALUES ('unrouted_probe'::text, "
            "'default'::text, '__null__'::text))"
        )
    return (
        "WITH "
        + ", ".join(ctes)
        + " SELECT * FROM (SELECT 'unrouted_probe'::text AS actor, 10::int AS residual) pac "
        "CROSS JOIN LATERAL (VALUES ('default'::text)) AS sq(queue_name) "
        f"CROSS JOIN LATERAL ({lateral.format(schema=schema)}) j"
    )


async def _explain_analyze(conn: asyncpg.Connection, sql: str, *params: object) -> str:
    rows = await conn.fetch(f"EXPLAIN (ANALYZE, BUFFERS) {sql}", *params)
    return "\n".join(r["QUERY PLAN"] for r in rows)


async def _explain(conn: asyncpg.Connection, sql: str, *params: object) -> str:
    rows = await conn.fetch(f"EXPLAIN (BUFFERS) {sql}", *params)
    return "\n".join(r["QUERY PLAN"] for r in rows)


@pytest.mark.integration
async def test_strict_fifo_lateral_probe_rides_the_unrouted_index(
    pg_dsn: str, mixed_population_schema: str
) -> None:
    """EXPLAIN ANALYZE the production strict-FIFO candidates lateral over
    the #243 shape: the probe must ride jobs_unrouted_actor_dispatch_idx
    (the marker is in the index predicate), so not one re-pended row is
    visited, the pending-only twin would walk all _REPEND_ROWS of them
    as "Rows Removed by Filter" before the first admissible row."""
    conn = await asyncpg.connect(pg_dsn)
    try:
        plan = await _explain_analyze(
            conn,
            _params_wrapper(
                _STRICT_FIFO_CANDIDATES_LATERAL, mixed_population_schema, rr_keys=False
            ),
            ["default"],
            10,
            new_uuid(),
            timedelta(seconds=30),
            2,
        )
        assert "jobs_unrouted_actor_dispatch_idx" in plan, (
            f"the strict-FIFO probe must ride the marker-partial index:\n{plan}"
        )
        assert "Rows Removed by Filter" not in plan, (
            "the probe walked rows it cannot admit: the NOT assignment_routed "
            "conjunct must be the index predicate, not a post-scan Filter:\n"
            f"{plan}"
        )
    finally:
        await conn.close()


@pytest.mark.integration
async def test_round_robin_lateral_probe_rides_the_unrouted_index(
    pg_dsn: str, mixed_population_schema: str
) -> None:
    """Same oracle for the round-robin candidates lateral (rr_keys
    literalized to the one seeded cohort): the probe must ride
    jobs_unrouted_round_robin_probe_idx and visit zero re-pended rows."""
    conn = await asyncpg.connect(pg_dsn)
    try:
        plan = await _explain_analyze(
            conn,
            _params_wrapper(_ROUND_ROBIN_CANDIDATES_LATERAL, mixed_population_schema, rr_keys=True),
            ["default"],
            10,
            new_uuid(),
            timedelta(seconds=30),
            2,
        )
        assert "jobs_unrouted_round_robin_probe_idx" in plan, (
            f"the round-robin probe must ride the marker-partial index:\n{plan}"
        )
        assert "Rows Removed by Filter" not in plan, (
            "the probe walked rows it cannot admit: the NOT assignment_routed "
            "conjunct must be the index predicate, not a post-scan Filter:\n"
            f"{plan}"
        )
    finally:
        await conn.close()


@pytest.mark.integration
async def test_full_dispatch_statements_pick_up_the_unrouted_indexes(
    pg_dsn: str, mixed_population_schema: str
) -> None:
    """EXPLAIN (no ANALYZE: the statements are UPDATEs) both production
    dispatch statements end to end: the marker-partial indexes appear in
    the plans, so the arms the laterals feed (capped and uncapped alike)
    inherit the depth bound."""
    conn = await asyncpg.connect(pg_dsn)
    try:
        for label, sql, expected in (
            ("strict_fifo", DISPATCH_STRICT_FIFO_SQL, "jobs_unrouted_actor_dispatch_idx"),
            ("round_robin", DISPATCH_ROUND_ROBIN_SQL, "jobs_unrouted_round_robin_probe_idx"),
        ):
            plan = await _explain(
                conn,
                sql.format(schema=mixed_population_schema),
                ["default"],
                10,
                new_uuid(),
                timedelta(seconds=30),
                2,
            )
            assert expected in plan, (
                f"{label}: the production statement's label-routed arm must "
                f"ride {expected}:\n{plan}"
            )
    finally:
        await conn.close()

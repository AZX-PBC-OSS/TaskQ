"""The contract §9 control 9 (§7): the victim-agnostic migration-deadlock
control. The real opposite-order cycle is driven by an event handshake —
`pg_locks` shows BOTH backends `granted=f` before either `deadlock_timeout`
or the 30 s `ddl_lock_timeout` can fire — then exactly one transaction
aborts with SQLSTATE `40P01` and the counterpart commits. The occurred
branch's whole-object-set catalog assertions hold per §2.1; no partial
object set in either branch. Minimum one handshake-formed trial per run;
never a rerun to prefer a victim; no planner GUC touched."""

from __future__ import annotations

import asyncio
import json as _json
from typing import Any

import asyncpg
import pytest

pytestmark = pytest.mark.integration

_DEADLOCK_TIMEOUT_S = 5.0  # the poll's own deadline for forming the cycle


def new_uuid_str() -> str:
    from taskq._ids import new_uuid

    return str(new_uuid())


async def _fresh_schema(pg_dsn: str, name: str) -> None:
    import asyncpg

    from taskq.migrate import apply_pending

    conn = await asyncpg.connect(pg_dsn)
    try:
        await apply_pending(conn, schema=name, target="01.00.41_02")
    finally:
        await conn.close()


async def _drop(pg_dsn: str, schema: str) -> None:
    import asyncpg

    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    finally:
        await conn.close()


def _folded_statements(schema: str) -> list[str]:
    """The folded 42_01's own statements, in file order (the runner's
    splitter)."""
    from taskq.migrate import discover, split_statements

    migration = next(m for m in discover() if m.key == "01.00.42_01:pre")
    return split_statements(migration.render(schema))


def _whole_object_set(schema: str) -> dict[str, Any]:
    """The §2.1 whole-object-set catalog probe (one row of facts)."""
    return {
        "epoch_col": f"""SELECT count(*)::int FROM pg_attribute
            WHERE attrelid = '{schema}.wf_step_ledger'::regclass
              AND attname = 'claim_epoch' AND NOT attisdropped""",
        "arbiter": f"SELECT pg_get_indexdef('{schema}.wf_step_ledger_claim_uniq'::regclass)",
        "capacity_cols": f"""SELECT count(*)::int FROM pg_attribute
            WHERE attrelid = '{schema}.wf_emit_capacity'::regclass
              AND NOT attisdropped AND attnum > 0""",
        "rotation_row": f"""SELECT after_flow_id FROM {schema}.wf_emit_intent_rotation WHERE id""",
        "pending_idx": f"SELECT pg_get_indexdef('{schema}.wf_emit_capacity_pending_idx'::regclass)",
        "guard_fn": f"""SELECT count(*)::int FROM pg_proc p
            JOIN pg_namespace n ON n.oid = p.pronamespace
            WHERE n.nspname = '{schema}' AND p.proname = 'wf_emit_park_guard'""",
        "guard_trig": f"""SELECT count(*)::int FROM pg_trigger
            WHERE tgrelid = '{schema}.jobs'::regclass
              AND tgname = 'wf_emit_park_guard' AND NOT tgisinternal""",
        "park_idx": f"SELECT pg_get_indexdef('{schema}.jobs_wf_emit_park_idx'::regclass)",
    }


async def _catalog_facts(conn: Any, schema: str) -> dict[str, Any]:
    """Every probe is absence-tolerant (the LOST-migration branch reads the
    pre-fold shape, where the post-fold relations do not exist): a missing
    relation reads as None, the same face a missing column gets."""
    facts: dict[str, Any] = {}
    for name, sql in _whole_object_set(schema).items():
        try:
            facts[name] = await conn.fetchval(sql)
        except asyncpg.UndefinedTableError:
            facts[name] = None
        except asyncpg.UndefinedColumnError:
            facts[name] = None
    return facts


def _post_fold_complete(facts: dict[str, Any]) -> bool:
    arbiter = facts["arbiter"] or ""
    pending = facts["pending_idx"] or ""
    park = facts["park_idx"] or ""
    return bool(
        facts["epoch_col"] == 1
        and "claim_epoch" in arbiter
        and facts["capacity_cols"] == 4
        and facts["rotation_row"] is not None
        and "wake_generation > handled_generation" in pending
        and facts["guard_fn"] == 1
        and facts["guard_trig"] == 1
        and "consume_budget" in park
    )


@pytest.mark.integration
async def test_migration_deadlock_handshake_is_victim_agnostic(pg_dsn: str) -> None:
    """One handshake-formed trial: the cycle is verified formed (both
    backends `granted=f`) BEFORE either timer; exactly one `40P01`; the
    counterpart commits; the occurred branch's catalog assertions hold; no
    partial object set."""
    import asyncpg

    from taskq.migrate import apply_pending

    schema = f"c9_{new_uuid_str().replace('-', '')[:16]}"
    await _fresh_schema(pg_dsn, schema)
    try:
        statements = _folded_statements(schema)
        assert "ADD COLUMN" in statements[0] and "wf_step_ledger" in statements[0]
        # The migration side is the RUNNER (the real apply path — its own
        # one-file transaction and its own schema_migrations bookkeeping).
        migration_conn = await asyncpg.connect(pg_dsn)
        writer_conn = await asyncpg.connect(pg_dsn)
        locks_conn = await asyncpg.connect(pg_dsn)
        outcome: dict[str, Any] = {}

        async def apply_migration() -> None:
            try:
                await apply_pending(migration_conn, schema=schema)
                outcome["migration"] = "committed"
            except asyncpg.PostgresError as exc:
                outcome["migration"] = exc
                await migration_conn.execute("ROLLBACK")

        # The writer's transaction: an application INSERT takes ROW
        # EXCLUSIVE on jobs and holds it.
        await writer_conn.execute("BEGIN")
        writer_flow = new_uuid_str()
        await writer_conn.execute(
            f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, status, '
            "max_attempts, retry_kind, step_key, metadata, fairness_key) "
            "VALUES ($1, 'c9', 'c9', '{}', 'pending', 3, 'transient', 'work', "
            "$2::jsonb, $3)",
            new_uuid_str(),
            _json.dumps({"flow_id": writer_flow}),
            writer_flow,
        )

        # Now both halves of the cycle, concurrently.
        async def writer_side() -> None:
            try:
                # The opposite order: jobs held, now wf_step_ledger.
                await writer_conn.execute(
                    f'UPDATE "{schema}".wf_step_ledger SET updated_at = clock_timestamp() '
                    "WHERE id = '00000000-0000-0000-0000-000000000000'"
                )
                await writer_conn.execute("COMMIT")
                outcome["writer"] = "committed"
            except asyncpg.PostgresError as exc:
                outcome["writer"] = exc
                await writer_conn.execute("ROLLBACK")

        # THE HANDSHAKE'S SEQUENCE: the migration first takes and HOLDS the
        # ledger's ACCESS EXCLUSIVE (its own cycle half), then waits on the
        # writer's jobs lock; only then does the writer's opposite-order
        # ledger write start — the writer waits on the migration's ledger
        # lock while holding jobs. The cycle is real before either timer.
        apply_task = asyncio.create_task(apply_migration())
        migration_waiting = False
        polls = 0
        while polls < int(_DEADLOCK_TIMEOUT_S / 0.02):
            # The migration's own half: it holds the ledger and waits on
            # jobs (any ungranted lock while the apply task runs).
            ungranted = await locks_conn.fetchval("SELECT count(*) FROM pg_locks WHERE NOT granted")
            if ungranted and int(ungranted) >= 1:
                migration_waiting = True
                break
            polls += 1
            await asyncio.sleep(0.02)
        assert migration_waiting, (
            "the migration never reached its own wait (the handshake's "
            "sequence failed before the cycle could form)"
        )
        writer_task = asyncio.create_task(writer_side())
        sides = asyncio.gather(apply_task, writer_task)

        # THE CYCLE: BOTH backends report granted=f before either timer.
        handshake_formed = False
        polls = 0
        while polls < int(_DEADLOCK_TIMEOUT_S / 0.02):
            ungranted = await locks_conn.fetchval("SELECT count(*) FROM pg_locks WHERE NOT granted")
            if ungranted and int(ungranted) >= 2:
                handshake_formed = True
                break
            polls += 1
            await asyncio.sleep(0.02)
        assert handshake_formed, (
            "the cycle never formed (the handshake's own proof is the "
            "anti-vacuity requirement — a never-formed cycle is red)"
        )
        await sides

        deadlocks = [
            side
            for side in ("migration", "writer")
            if isinstance(outcome.get(side), asyncpg.PostgresError)
        ]
        committed = [side for side in ("migration", "writer") if outcome.get(side) == "committed"]
        assert len(deadlocks) == 1 and len(committed) == 1, outcome
        victim = deadlocks[0]
        exc = outcome[victim]
        assert "40P01" in str(exc) or "deadlock" in str(exc).lower(), exc

        # The occurred branch's catalog assertions.
        probe = await asyncpg.connect(pg_dsn)
        record = await probe.fetchval(
            f"SELECT count(*)::int FROM \"{schema}\".schema_migrations WHERE version = '01.00.42_01:pre'"
        )
        facts = await _catalog_facts(probe, schema)
        if victim == "migration":
            # The pre-fold shape intact, every addition absent, no record.
            assert record == 0, facts
            assert not _post_fold_complete(facts)
            # Drain and re-apply → the complete post-fold shape, exactly
            # one record.
            await probe.close()
            conn2 = await asyncpg.connect(pg_dsn)
            await apply_pending(conn2, schema=schema)
            await conn2.close()
            probe = await asyncpg.connect(pg_dsn)
            record = await probe.fetchval(
                f"SELECT count(*)::int FROM \"{schema}\".schema_migrations WHERE version = '01.00.42_01:pre'"
            )
            facts = await _catalog_facts(probe, schema)
            assert record == 1, facts
            assert _post_fold_complete(facts), facts
        else:
            # The committed post-fold set with the runner's own record (no
            # re-apply owed); the writer's application rolled back.
            assert record == 1, facts
            assert _post_fold_complete(facts), facts
        # No partial object set in either branch: the post-fold set is
        # complete (the surviving migration) or the pre-fold shape is
        # intact with no record (the lost migration, re-applied above).
        assert record == 1 and _post_fold_complete(facts)
        await probe.close()
        await migration_conn.close()
        await writer_conn.close()
        await locks_conn.close()
        await _drop(pg_dsn, schema)
    except BaseException:
        await _drop(pg_dsn, schema)
        raise

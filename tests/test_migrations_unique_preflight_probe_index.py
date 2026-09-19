"""Pins for the unique-preflight probe index (01.00.15_01).

The unique_for preflight (``enqueue_unique_for_preflight``) probes the
newest job for a caller-configurable ``unique_states`` set. The only
index that served it was partial on the ACTIVE status triple, so any
actor folding a terminal state into ``unique_states`` (the documented
``succeeded`` dedup use) degraded the probe, on the enqueue path, to a
sequential scan. Migration 01.00.15_01 adds the non-partial
``(actor, identity_key, status)`` index the probe needs for ANY status
set; these pins hold the migration to that contract, following the
index-audit idiom (migration pins plus EXPLAIN plan pins on a seeded
schema, as tests/test_index_audit.py and tests/test_postgres_unique_for.py
establish).
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import asyncpg
import pytest

from taskq import migrate as migrate_mod
from taskq._ids import new_uuid
from taskq.backend._sql_templates import render
from taskq.settings import TaskQSettings

pytestmark = pytest.mark.integration

_INDEX_NAME = "jobs_identity_status_idx"


async def test_index_exists_and_is_non_partial(
    pg_conn: asyncpg.Connection, settings: TaskQSettings
) -> None:
    await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)
    row = await pg_conn.fetchrow(
        """
        SELECT indexdef
        FROM pg_indexes
        WHERE schemaname = $1 AND indexname = $2
        """,
        settings.schema_name,
        _INDEX_NAME,
    )
    assert row is not None, (
        "the unique-preflight probe index is missing; every probe whose "
        "unique_states leaves the active triple degrades to a sequential "
        "scan on the enqueue path"
    )
    indexdef: str = row["indexdef"]
    # Non-partial is the point: the probe's status set is caller
    # configuration, and a partial index on any status subset is exactly
    # the defect this migration fixes.
    assert "WHERE" not in indexdef, (
        f"{_INDEX_NAME} must not be partial: a partial index cannot serve "
        "a probe whose unique_states set the planner cannot prove is a "
        "subset of the predicate (that is the defect being fixed)"
    )
    for column in ("actor", "identity_key", "status"):
        assert column in indexdef, f"{_INDEX_NAME} must key on {column}"


async def _seed_identity_corpus(
    conn: asyncpg.Connection, schema: str, actor: str, identity: str
) -> None:
    """Seed a modest varied corpus plus one terminal match row.

    Varied identities keep the corpus from collapsing into one index
    entry; the exact-match row is terminal, the shape the partial
    active-row index cannot serve.
    """
    rows: list[tuple[object, ...]] = []
    for i in range(500):
        rows.append(
            (
                new_uuid(),
                f"seed_actor_{i % 50}",
                "default",
                f"seed_ident_{i}",
                "{}",
                3,
                "transient",
                "succeeded",
                datetime.now(UTC),
            )
        )
    await conn.copy_records_to_table(
        "jobs",
        schema_name=schema,
        records=rows,
        columns=(
            "id",
            "actor",
            "queue",
            "identity_key",
            "payload",
            "max_attempts",
            "retry_kind",
            "status",
            "scheduled_at",
        ),
    )
    await conn.execute(
        f'INSERT INTO "{schema}".jobs '  # noqa: S608  # Why: schema is fixture-derived and validated by the migration runner; every value is $N-bound
        "(id, actor, queue, identity_key, payload, max_attempts, retry_kind, "
        "status, scheduled_at) "
        "VALUES ($1, $2, 'default', $3, '{}'::jsonb, 3, 'transient', "
        "'succeeded', clock_timestamp())",
        new_uuid(),
        actor,
        identity,
    )
    await conn.execute(f'ANALYZE "{schema}".jobs')


async def test_preflight_with_terminal_states_uses_the_new_index(
    pg_conn: asyncpg.Connection, settings: TaskQSettings
) -> None:
    await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)
    schema = settings.schema_name
    actor = "_unique_preflight_probe_actor"
    identity = "probe-identity"
    await _seed_identity_corpus(pg_conn, schema, actor, identity)

    template = render(schema)
    # The exact production preflight statement, with a terminal status in
    # the caller's unique_states array: the shape where the partial
    # active-row index cannot apply.
    sql = template.enqueue_unique_for_preflight
    async with pg_conn.transaction():
        # Scoped to a transaction so SET LOCAL does not affect other
        # tests (the same discipline test_postgres_unique_for.py uses).
        await pg_conn.execute("SET LOCAL enable_seqscan = off")
        rec = await pg_conn.fetchrow(
            f"EXPLAIN (FORMAT JSON) {sql}",  # Why: the statement is the module's own validated production template, values are $N-bound
            actor,
            identity,
            ["pending", "succeeded"],
            timedelta(minutes=15),
        )
    assert rec is not None, "EXPLAIN returned no rows"
    plan_text: str = rec[0]  # type: ignore[reportOptionalSubscript]  # Why: guarded by assert above
    plan_json = json.dumps(json.loads(plan_text), default=str)

    assert _INDEX_NAME in plan_json, (
        f"the terminal-status preflight probe did not use {_INDEX_NAME}; "
        f"the sequential-scan degradation is back. Plan: {plan_json}"
    )
    assert "jobs_identity_active_idx" not in plan_json, (
        "the plan used the partial active-row index for a probe carrying a "
        "terminal status; if that index can serve this probe the partial "
        "predicate's proof is wrong, and the pin's premise is broken"
    )


async def test_preflight_with_terminal_states_finds_the_terminal_row(
    pg_conn: asyncpg.Connection, settings: TaskQSettings
) -> None:
    """The probe's ANSWER is right for a terminal status set: a recent
    succeeded row is the dedup match, the documented ``succeeded``
    unique_states use."""
    await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)
    schema = settings.schema_name
    actor = "_unique_preflight_answer_actor"
    identity = "answer-identity"
    await _seed_identity_corpus(pg_conn, schema, actor, identity)

    template = render(schema)
    rec = await pg_conn.fetchrow(
        template.enqueue_unique_for_preflight,
        actor,
        identity,
        ["pending", "succeeded"],
        timedelta(minutes=15),
    )
    assert rec is not None, (
        "the preflight probe returned no row for a terminal status set: "
        "the unique dedup would enqueue a duplicate while the succeeded "
        "match sits inside the window"
    )
    assert rec["actor"] == actor
    assert str(rec["identity_key"]) == identity
    assert str(rec["status"]) == "succeeded"

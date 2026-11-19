# Why: every query's schema identifier comes from the fixture-validated schema name and every value is $-bound; only the f-string interpolation of the schema is flagged.
"""The D4 capability-preflight negative controls: the catalog preflight
compares EXACT definitions (``pg_get_indexdef``, normalized, full
equality) — never substrings.

The defect this control pins: the preflight's arbiter check was a
five-NAME substring scan and the park-index check a three-token
substring scan, so a WRONG same-name index — a five-expression arbiter
whose claim then fails 42P10 at the ON CONFLICT insert, or a
wrong-column park index whose probe then plans a Seq Scan on jobs —
was accepted by the preflight and failed only later, at feature use,
in the claim's or the probe's own path. A same-name wrong object is
exactly the shape ``CREATE INDEX IF NOT EXISTS`` cannot detect; the
preflight's exact-definition equality is the enforcement point.

The constraint class (the final whole-diff review's finding, KF-2):
the capacity table's CHECK check was a bare COUNT of ``contype='c'``
rows — it accepted at least-two CHECK constraints of ANY body. The
executed exploit on real PostgreSQL 18.7: drop both shipped CHECKs and
recreate them under their ORIGINAL names as ``CHECK (true)`` — the
preflight accepted, and PostgreSQL then accepted ``bound = 0`` and
``wake_generation = 0, handled_generation = 1``. The preflight now
validates each required constraint by NAME, contype, and EXACT
normalized body (``pg_get_constraintdef``, whitespace/quote/paren
normalised), and the rotation singleton's whole shape the same way;
a missing, renamed, wrong-contype, unparseable, or same-name
wrong-body constraint is a refusal, never a pass.

Each control: apply the real folded migration to a fresh schema,
replace one object with a plausible wrong definition, reset the
capability cache (the per-schema verdict is a process-lifetime fact
about the catalog's SHAPE — a replaced object must re-verify), and
assert the REAL ``verify_emit_capacity_objects`` refuses with the
typed capability error. The positive arm re-asserts the migrated
schema's own definitions pass, so the constants cannot drift away from
PostgreSQL's real ``pg_get_indexdef``/``pg_get_constraintdef`` output
format without a red.
"""

from __future__ import annotations

import asyncpg
import pytest

from taskq.migrate import apply_pending
from taskq.workflows._capability import (
    WorkflowWorkerCapabilityError,
    reset_emit_capability_cache,
    verify_emit_capacity_objects,
)
from taskq.workflows.engine import render_workflow_sql

pytestmark = pytest.mark.integration


async def _fresh_migrated(conn: asyncpg.Connection, schema: str) -> None:
    await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    await conn.execute(f'CREATE SCHEMA "{schema}"')
    await apply_pending(conn, schema=schema)


async def test_preflight_accepts_the_real_migrated_definitions_exactly(
    pg_dsn: str,
) -> None:
    """The positive arm: the real migration's own object set satisfies the
    exact-definition preflight — the constants track PostgreSQL's real
    ``pg_get_indexdef`` output format (a format drift reds here)."""
    schema = "cap4_positive_probe"
    conn = await asyncpg.connect(pg_dsn)
    try:
        await _fresh_migrated(conn, schema)
        reset_emit_capability_cache(schema)
        wsql = render_workflow_sql(schema)
        await verify_emit_capacity_objects(conn, schema)
        # The cache took the verdict (a second check is the cached path).
        await verify_emit_capacity_objects(conn, schema)
        assert wsql is not None
    finally:
        reset_emit_capability_cache(schema)
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


async def test_preflight_refuses_a_wrong_same_name_arbiter(pg_dsn: str) -> None:
    """A five-expression arbiter that carries every arbiter NAME but
    drops ``claim_epoch``'s unique enforcement (a COALESCE-wrapped
    ``claim_epoch + 0`` — the review's own wrong shape) must refuse
    BEFORE any feature use: the real claim's ON CONFLICT would fail
    42P10 against it."""
    schema = "cap4_wrong_arbiter"
    conn = await asyncpg.connect(pg_dsn)
    try:
        await _fresh_migrated(conn, schema)
        await conn.execute(f'DROP INDEX "{schema}".wf_step_ledger_claim_uniq')
        await conn.execute(
            f'CREATE UNIQUE INDEX wf_step_ledger_claim_uniq ON "{schema}".wf_step_ledger '
            "USING btree (flow_id, step_key, COALESCE((map_index)::integer, '-1'::integer), "
            "attempt, (claim_epoch + 0))"
        )
        reset_emit_capability_cache(schema)
        with pytest.raises(WorkflowWorkerCapabilityError, match="wf_step_ledger_claim_uniq"):
            await verify_emit_capacity_objects(conn, schema)
        reset_emit_capability_cache(schema)
    finally:
        reset_emit_capability_cache(schema)
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


async def test_preflight_refuses_a_wrong_same_name_park_index(pg_dsn: str) -> None:
    """A same-name park index over the wrong key columns (every
    substring token present: the review's own wrong shape) must refuse —
    its probe planned a Seq Scan on jobs and its candidate fell back to
    the flow-nodes index."""
    schema = "cap4_wrong_park_idx"
    conn = await asyncpg.connect(pg_dsn)
    try:
        await _fresh_migrated(conn, schema)
        await conn.execute(f'DROP INDEX "{schema}".jobs_wf_emit_park_idx')
        await conn.execute(
            f'CREATE INDEX jobs_wf_emit_park_idx ON "{schema}".jobs '
            "USING btree (((metadata->>'flow_id')::uuid), created_at, id, step_key) "
            f"WHERE status = 'scheduled'::\"{schema}\".job_status "
            'AND (metadata @> \'{"blocking_reason": "backpressure"}\'::jsonb) '
            'AND (metadata @> \'{"wf_deferral": {"signal": "EmitBackpressure", '
            '"attempt": 0, "consume_budget": false}}\'::jsonb) '
            'AND (metadata @> \'{"wf_emit_backpressure": {"version": 1}}\'::jsonb) '
            "AND (step_key IS NOT NULL) AND (step_key <> '__flow__'::text) "
            "AND (metadata ? 'flow_id'::text)"
        )
        reset_emit_capability_cache(schema)
        with pytest.raises(WorkflowWorkerCapabilityError, match="jobs_wf_emit_park_idx"):
            await verify_emit_capacity_objects(conn, schema)
        reset_emit_capability_cache(schema)
    finally:
        reset_emit_capability_cache(schema)
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


async def test_preflight_refuses_a_wrong_same_name_pending_index(pg_dsn: str) -> None:
    """A same-name pending index over the wrong predicate must refuse."""
    schema = "cap4_wrong_pending_idx"
    conn = await asyncpg.connect(pg_dsn)
    try:
        await _fresh_migrated(conn, schema)
        await conn.execute(f'DROP INDEX "{schema}".wf_emit_capacity_pending_idx')
        await conn.execute(
            f'CREATE INDEX wf_emit_capacity_pending_idx ON "{schema}".wf_emit_capacity '
            "USING btree (flow_id) WHERE (wake_generation >= 0)"
        )
        reset_emit_capability_cache(schema)
        with pytest.raises(WorkflowWorkerCapabilityError, match="wf_emit_capacity_pending_idx"):
            await verify_emit_capacity_objects(conn, schema)
        reset_emit_capability_cache(schema)
    finally:
        reset_emit_capability_cache(schema)
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


async def test_preflight_refuses_both_capacity_checks_recreated_as_check_true(
    pg_dsn: str,
) -> None:
    """THE EXECUTED EXPLOIT (the review's own shape, on real PostgreSQL):
    drop both shipped CHECK constraints and recreate them under their
    ORIGINAL names as ``CHECK (true)``. The count-of-CHECKs preflight
    accepted this (two CHECK constraints exist), and PostgreSQL then
    accepted ``bound = 0`` and ``wake_generation = 0,
    handled_generation = 1`` — the promotion-ordering invariant gone.
    The preflight must refuse by name and body."""
    schema = "cap4_check_true_pair"
    conn = await asyncpg.connect(pg_dsn)
    try:
        await _fresh_migrated(conn, schema)
        for name in (
            "wf_emit_capacity_bound_positive",
            "wf_emit_capacity_generations_monotonic",
        ):
            await conn.execute(f'ALTER TABLE "{schema}".wf_emit_capacity DROP CONSTRAINT {name}')
            await conn.execute(
                f'ALTER TABLE "{schema}".wf_emit_capacity ADD CONSTRAINT {name} CHECK (true)'
            )
        # The corruption is real, not hypothetical: the emptied bodies
        # accept exactly the rows the invariants forbid.
        await conn.execute(
            f'INSERT INTO "{schema}".wf_emit_capacity '  # noqa: S608  # Why: the fixture-validated throwaway schema identifier; every value is a literal probe of the emptied invariants.
            "(flow_id, bound, wake_generation, handled_generation) "
            "VALUES (gen_random_uuid(), 0, 0, 1)"
        )
        reset_emit_capability_cache(schema)
        with pytest.raises(WorkflowWorkerCapabilityError, match="wf_emit_capacity_bound_positive"):
            await verify_emit_capacity_objects(conn, schema)
        reset_emit_capability_cache(schema)
    finally:
        reset_emit_capability_cache(schema)
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


async def test_preflight_refuses_a_semantically_inverted_generation_check(
    pg_dsn: str,
) -> None:
    """The monotonic constraint's body INVERTED (``<=`` — which legalises
    ``handled_generation`` overtaking ``wake_generation``, the exact
    corruption the real constraint forbids) under its own original name
    must refuse: the preflight compares the body, never the count."""
    schema = "cap4_inverted_check"
    conn = await asyncpg.connect(pg_dsn)
    try:
        await _fresh_migrated(conn, schema)
        await conn.execute(
            f'ALTER TABLE "{schema}".wf_emit_capacity '
            "DROP CONSTRAINT wf_emit_capacity_generations_monotonic"
        )
        await conn.execute(
            f'ALTER TABLE "{schema}".wf_emit_capacity '
            "ADD CONSTRAINT wf_emit_capacity_generations_monotonic "
            "CHECK (wake_generation <= handled_generation)"
        )
        reset_emit_capability_cache(schema)
        with pytest.raises(
            WorkflowWorkerCapabilityError, match="wf_emit_capacity_generations_monotonic"
        ):
            await verify_emit_capacity_objects(conn, schema)
        reset_emit_capability_cache(schema)
    finally:
        reset_emit_capability_cache(schema)
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


async def test_preflight_refuses_a_near_miss_bound_check(pg_dsn: str) -> None:
    """The bound constraint's near-miss body (``bound >= 0`` — the one
    difference being the admitted ``bound = 0`` row) must refuse."""
    schema = "cap4_nearmiss_bound"
    conn = await asyncpg.connect(pg_dsn)
    try:
        await _fresh_migrated(conn, schema)
        await conn.execute(
            f'ALTER TABLE "{schema}".wf_emit_capacity '
            "DROP CONSTRAINT wf_emit_capacity_bound_positive"
        )
        await conn.execute(
            f'ALTER TABLE "{schema}".wf_emit_capacity '
            "ADD CONSTRAINT wf_emit_capacity_bound_positive CHECK (bound >= 0)"
        )
        reset_emit_capability_cache(schema)
        with pytest.raises(WorkflowWorkerCapabilityError, match="wf_emit_capacity_bound_positive"):
            await verify_emit_capacity_objects(conn, schema)
        reset_emit_capability_cache(schema)
    finally:
        reset_emit_capability_cache(schema)
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


async def test_preflight_refuses_a_renamed_but_correct_bodied_check(pg_dsn: str) -> None:
    """The bound constraint's EXACT body under a DIFFERENT name must
    refuse too — the migration's own object set is named, and a renamed
    constraint is exactly what ``IF NOT EXISTS``-shaped re-application
    would produce against a drifted schema."""
    schema = "cap4_renamed_check"
    conn = await asyncpg.connect(pg_dsn)
    try:
        await _fresh_migrated(conn, schema)
        await conn.execute(
            f'ALTER TABLE "{schema}".wf_emit_capacity '
            "DROP CONSTRAINT wf_emit_capacity_bound_positive"
        )
        await conn.execute(
            f'ALTER TABLE "{schema}".wf_emit_capacity '
            "ADD CONSTRAINT wf_emit_capacity_bound_positive_v2 CHECK (bound > 0)"
        )
        reset_emit_capability_cache(schema)
        with pytest.raises(WorkflowWorkerCapabilityError, match="wf_emit_capacity_bound_positive"):
            await verify_emit_capacity_objects(conn, schema)
        reset_emit_capability_cache(schema)
    finally:
        reset_emit_capability_cache(schema)
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


async def test_preflight_refuses_a_unique_constraint_shadowing_a_check_name(
    pg_dsn: str,
) -> None:
    """A dropped CHECK's name is FREE (PostgreSQL requires constraint-name
    uniqueness per table, across types): a UNIQUE constraint reusing
    ``wf_emit_capacity_generations_monotonic`` carries the name, and a
    decoy third CHECK keeps the contype='c' COUNT at two — the count
    preflight accepted exactly this shape (the name matched, the count
    matched) while the invariant was gone. The name-matched body
    comparison refuses it (wrong contype)."""
    schema = "cap4_unique_shadow"
    conn = await asyncpg.connect(pg_dsn)
    try:
        await _fresh_migrated(conn, schema)
        await conn.execute(
            f'ALTER TABLE "{schema}".wf_emit_capacity '
            "DROP CONSTRAINT wf_emit_capacity_generations_monotonic"
        )
        await conn.execute(
            f'ALTER TABLE "{schema}".wf_emit_capacity '
            "ADD CONSTRAINT wf_emit_capacity_generations_monotonic "
            "UNIQUE (wake_generation)"
        )
        # The decoy keeps the CHECK count at two — the count law must
        # not be this control's refuser; the name/contype/body rigour is.
        await conn.execute(
            f'ALTER TABLE "{schema}".wf_emit_capacity '
            "ADD CONSTRAINT wf_emit_capacity_decoy CHECK (true)"
        )
        reset_emit_capability_cache(schema)
        with pytest.raises(
            WorkflowWorkerCapabilityError, match="wf_emit_capacity_generations_monotonic"
        ):
            await verify_emit_capacity_objects(conn, schema)
        reset_emit_capability_cache(schema)
    finally:
        reset_emit_capability_cache(schema)
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


async def test_preflight_refuses_a_dropped_capacity_check(pg_dsn: str) -> None:
    """A CHECK dropped and NOT recreated must refuse by name. The decoy
    third CHECK keeps the contype='c' COUNT at two — the count law must
    not be this control's refuser; the missing NAME is."""
    schema = "cap4_dropped_check"
    conn = await asyncpg.connect(pg_dsn)
    try:
        await _fresh_migrated(conn, schema)
        await conn.execute(
            f'ALTER TABLE "{schema}".wf_emit_capacity '
            "DROP CONSTRAINT wf_emit_capacity_bound_positive"
        )
        await conn.execute(
            f'ALTER TABLE "{schema}".wf_emit_capacity '
            "ADD CONSTRAINT wf_emit_capacity_decoy CHECK (true)"
        )
        reset_emit_capability_cache(schema)
        with pytest.raises(WorkflowWorkerCapabilityError, match="wf_emit_capacity_bound_positive"):
            await verify_emit_capacity_objects(conn, schema)
        reset_emit_capability_cache(schema)
    finally:
        reset_emit_capability_cache(schema)
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


async def test_preflight_refuses_the_rotation_singleton_check_true(pg_dsn: str) -> None:
    """The rotation singleton's own CHECK recreated as ``CHECK (true)``
    admits a second ``id = false`` row beside the singleton — the
    'never grows with run cardinality' law's enforcement gone. The
    singleton shape is validated with the same name+body rigour."""
    schema = "cap4_rotation_check_true"
    conn = await asyncpg.connect(pg_dsn)
    try:
        await _fresh_migrated(conn, schema)
        await conn.execute(
            f'ALTER TABLE "{schema}".wf_emit_intent_rotation '
            "DROP CONSTRAINT wf_emit_intent_rotation_singleton"
        )
        await conn.execute(
            f'ALTER TABLE "{schema}".wf_emit_intent_rotation '
            "ADD CONSTRAINT wf_emit_intent_rotation_singleton CHECK (true)"
        )
        # The corruption is real: a second row fits beside the singleton.
        await conn.execute(
            f'INSERT INTO "{schema}".wf_emit_intent_rotation (id) VALUES (false)'  # noqa: S608  # Why: the fixture-validated throwaway schema identifier; the value is a literal probe of the emptied singleton invariant.
        )
        reset_emit_capability_cache(schema)
        with pytest.raises(
            WorkflowWorkerCapabilityError, match="wf_emit_intent_rotation_singleton"
        ):
            await verify_emit_capacity_objects(conn, schema)
        reset_emit_capability_cache(schema)
    finally:
        reset_emit_capability_cache(schema)
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()

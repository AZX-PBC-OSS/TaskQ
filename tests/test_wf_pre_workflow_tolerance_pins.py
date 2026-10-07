"""The pre-workflow-schema tolerance pins (the phase-2 attack's M1 cure):
the T18 workflow-liveness guard probes ``wf_edge`` — a schema the
workflow round has NOT landed on has no such table, and an untolerated
miss is the leader's death (a non-transient error class fatal to the
sweep loop).

The shipped tolerance, pinned END-TO-END against a REAL pre-workflow
schema (the migrations applied to target ``01.00.22_01`` — jobs, the
archive pair, the prune watermark; NO workflow tables):

* the result-expiry arm survives the missing tables by RUNNING — the
  unguarded fallback (semantically exact there: no join-wait rows to
  hold for), logged once per process — never by failing per tick (the
  doc's old claim — "the per-tick tolerance, the expiry does not run" —
  described a mechanism that did not exist; the hold direction held only
  by accident of the failure);
* the per-actor prune arm survives it the same way (a pre-workflow
  schema with ``actor_overrides`` configured killed the leader before
  the tolerance);
* the convicted variant (the tolerance dropped: the guarded statement
  run raw) REDS with the exact ``UndefinedTableError`` — the crash the
  tolerance exists to prevent, observed on this very schema.
"""

from __future__ import annotations

from datetime import timedelta

import asyncpg
import pytest

from taskq.backend._sweeps import (
    _SWEEP_RESULT_TTL_SQL,  # pyright: ignore[reportPrivateUsage]  # Why: the convicted variant IS the guarded statement run raw — the mutation is the drill.
    sweep_expired_results,
)
from taskq.migrate import apply_pending
from taskq.worker._leader_shared import prune_terminal_jobs

#: The crash window's own shape: the workflow round's COLUMN file applied
#: (01.00.23_01 — the jobs/jobs_archive column mirrors exist, so the
#: archive write runs), the TABLE file pending (01.00.23_02 — NO wf_edge
#: anywhere). The three files apply in one `migrate up --phase pre` run,
#: each in its own transaction, so a crash between them leaves exactly
#: this schema — the window the tolerances exist for.
PRE_WORKFLOW_TARGET = "01.00.23_01"


async def _pre_workflow_schema(admin_conn: asyncpg.Connection, name: str) -> str:
    await admin_conn.execute(f'DROP SCHEMA IF EXISTS "{name}" CASCADE')
    await admin_conn.execute(f'CREATE SCHEMA "{name}"')
    await apply_pending(admin_conn, schema=name, target=PRE_WORKFLOW_TARGET)
    return name


async def _seed_expired_result(
    conn: asyncpg.Connection, schema: str, *, actor: str, key: str
) -> None:
    # NOTE: a PRE-workflow jobs table has NO step_key column (the column
    # lands with the workflow round, 01.00.23_01) — the seed uses the
    # schema's own columns only.
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, '
        "retry_kind, status, attempt, result, result_expires_at, finished_at, "
        "idempotency_scope, idempotency_key) "
        "VALUES (gen_random_uuid(), $1, 'default', '{}', 3, 'transient', 'succeeded', 1, "
        "'{\"ok\": 1}'::jsonb, now() - interval '1 second', "
        "now() - interval '5 days', 'scope', $2)",
        actor,
        key,
    )


@pytest.mark.integration
async def test_wf_pre_workflow_expiry_tolerance(
    clean_pg_conn: asyncpg.Connection, module_pg_schema: pytest.FixtureRequest
) -> None:
    """The result-expiry arm on a schema with NO workflow tables: the arm
    RUNS (the unguarded fallback — exact there) and expires the row; the
    convicted variant (the guarded statement raw — the tolerance dropped)
    raises the leader-killing miss."""
    pre = module_pg_schema.schema_name + "_prewf"
    schema_name = await _pre_workflow_schema(clean_pg_conn, pre)
    await _seed_expired_result(clean_pg_conn, schema_name, actor="van", key="tol-1")

    # THE SHIPPED ARM: tolerated — the expiry runs unguarded.
    expired = await sweep_expired_results(clean_pg_conn, schema=schema_name)
    assert expired >= 1, f"the tolerance must let the expiry RUN: {expired}"
    left = await clean_pg_conn.fetchval(
        f'SELECT count(*) FROM "{schema_name}".jobs WHERE result IS NOT NULL'
    )
    assert left == 0, "the unguarded fallback expires the row (exact on this schema)"

    # THE CONVICTED VARIANT (the tolerance dropped): the guarded statement
    # run raw on this schema — the leader-killing miss, observed.
    with pytest.raises(asyncpg.UndefinedTableError):
        await clean_pg_conn.execute(_SWEEP_RESULT_TTL_SQL.format(schema=schema_name), 100)


@pytest.mark.integration
async def test_wf_pre_workflow_actor_prune_tolerance(
    clean_pg_conn: asyncpg.Connection, module_pg_schema: pytest.FixtureRequest
) -> None:
    """The per-actor prune arm on a schema with NO workflow tables and an
    actor_overrides entry: the arm survives (the per-batch unguarded
    fallback — the pre-tolerance shape was the leader's death) and prunes
    the actor's aged rows; the convicted variant (the guarded actor
    candidate raw) raises the miss."""
    pre = module_pg_schema.schema_name + "_prewf2"
    schema_name = await _pre_workflow_schema(clean_pg_conn, pre)
    await _seed_expired_result(clean_pg_conn, schema_name, actor="override_me", key="tol-2")

    result = await prune_terminal_jobs(
        clean_pg_conn,
        retention_per_status={"succeeded": timedelta(days=30)},
        archive_retention=timedelta(days=365),
        batch_size=100,
        schema=schema_name,
        actor_overrides={"override_me": timedelta(days=1)},
    )
    assert result.total_deleted >= 1, f"the actor arm must survive and prune: {result}"

    # THE CONVICTED VARIANT (the tolerance dropped): the guarded ACTOR
    # candidate run raw on this schema — the pre-tolerance leader-killer.
    from taskq.worker._leader_shared import (
        _ARCHIVE_CANDIDATE_ACTOR_SQL,  # pyright: ignore[reportPrivateUsage]
    )

    with pytest.raises(asyncpg.UndefinedTableError):
        await clean_pg_conn.fetch(
            _ARCHIVE_CANDIDATE_ACTOR_SQL.format(schema=schema_name),
            "succeeded",
            timedelta(days=1),
            100,
            "override_me",
        )

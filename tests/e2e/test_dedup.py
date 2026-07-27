"""``unique_for`` dedup e2e — same identity dedupes, distinct identities both run.

Design spec scenario row (docs/superpowers/specs/2026-07-27-e2e-test-suite-design.md):
``rebuild_search_index`` is decorated with ``unique_for=timedelta(minutes=10)``;
dedup fires only when the enqueue also passes an ``identity_key``. Two enqueues
with the same identity inside the window → single execution (one effects row);
the second handle's ``was_existing`` is True.

Dedup mechanics, verified against the library (not guessed):

- ``build_enqueue_args`` falls back to the actor-declared
  ``ref.unique_for``/``ref.unique_states`` when the call passes neither
  (``client/_args.py``), so the tests only supply ``identity_key``.
- The backend preflight runs only when BOTH ``unique_for`` and
  ``identity_key`` are set (``backend/_enqueue.py._enqueue_on_conn``); the SQL
  matches the newest active row — ``status = ANY(unique_states)``, default
  ``pending``/``scheduled``/``running`` — within
  ``created_at > now() - unique_for``
  (``backend/_sql_templates.py.enqueue_unique_for_preflight``).
- On a preflight hit the existing row is returned and no INSERT happens; the
  client flags the handle ``was_existing = (row.id != args.id)``
  (``client/_jobs.py``).
- The two enqueues below are back-to-back awaits: the first row is committed
  ``pending`` before the first ``enqueue()`` returns, and the worker cannot
  complete the 50 ms actor before the second enqueue's preflight runs — the
  dedup hit is deterministic, not a race.

Every test requests ``e2e_worker`` explicitly: the worker container fixture is
not autouse, so no worker (and no dispatch) exists unless a test pulls it in.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest

from taskq import IdentityKey

from ._assertions import fetch_effects
from .actors import RebuildSearchIndexPayload, rebuild_search_index

if TYPE_CHECKING:
    import asyncpg

    from taskq import TaskQ

    from .conftest import E2ESchema, E2EWorker

pytestmark = [pytest.mark.e2e, pytest.mark.timeout(900)]


async def test_unique_for_dedupes_second_enqueue(
    e2e_client: TaskQ,
    e2e_worker: E2EWorker,
    e2e_pg_pool: asyncpg.Pool,
    e2e_schema: E2ESchema,
    run_id: str,
) -> None:
    """Same (actor, identity_key) twice inside the window → one job, one execution.

    Both handles resolve to the same terminal row: ``wait()`` on either
    observes the single job's success, and the effects table holds exactly
    one ``rebuilt`` row for the run (double-execution absence).
    """
    payload = RebuildSearchIndexPayload(run_id=run_id, index_name=f"idx-{run_id[:8]}")
    identity = IdentityKey(f"rebuild-{run_id[:12]}")

    first = await e2e_client.enqueue(rebuild_search_index, payload, identity_key=identity)
    second = await e2e_client.enqueue(rebuild_search_index, payload, identity_key=identity)

    assert first.was_existing is False
    assert second.was_existing is True
    assert second.job_id == first.job_id

    await first.wait(timeout=60)
    await second.wait(timeout=60)

    rows = await fetch_effects(e2e_pg_pool, e2e_schema.schema_name, run_id, kind="rebuilt")
    assert len(rows) == 1
    assert rows[0]["job_id"] == first.job_id
    assert rows[0]["attempt"] == 1


async def test_distinct_identity_keys_both_run(
    e2e_client: TaskQ,
    e2e_worker: E2EWorker,
    e2e_pg_pool: asyncpg.Pool,
    e2e_schema: E2ESchema,
    run_id: str,
) -> None:
    """Different identity_keys → no dedup: two jobs, two executions.

    The pair shares ``run_id`` so both effects correlate to this test; the
    per-effect ``index_name`` detail proves each identity ran its own payload.
    """
    handle_a = await e2e_client.enqueue(
        rebuild_search_index,
        RebuildSearchIndexPayload(run_id=run_id, index_name="idx-a"),
        identity_key=IdentityKey(f"rebuild-a-{run_id[:12]}"),
    )
    handle_b = await e2e_client.enqueue(
        rebuild_search_index,
        RebuildSearchIndexPayload(run_id=run_id, index_name="idx-b"),
        identity_key=IdentityKey(f"rebuild-b-{run_id[:12]}"),
    )

    assert handle_a.was_existing is False
    assert handle_b.was_existing is False
    assert handle_a.job_id != handle_b.job_id

    await handle_a.wait(timeout=60)
    await handle_b.wait(timeout=60)

    rows = await fetch_effects(e2e_pg_pool, e2e_schema.schema_name, run_id, kind="rebuilt")
    assert len(rows) == 2
    assert {row["job_id"] for row in rows} == {handle_a.job_id, handle_b.job_id}
    details: list[dict[str, str]] = [json.loads(row["detail"]) for row in rows]
    assert {detail["index_name"] for detail in details} == {"idx-a", "idx-b"}

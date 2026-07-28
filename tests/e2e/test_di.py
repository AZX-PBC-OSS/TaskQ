"""DI e2e — provider bootstrap inside a real worker container.

Scenario:
``enrich_order`` → effects include injected-http fetch + pool write; proves
DI bootstrap inside a real worker container. Ground truth: ``e2e_effects``
plus the ``jobs`` error columns on the failure path.

Providers under test (tests/e2e/di.py, registered by ``worker_entry``):
``asyncpg.Pool`` at LOOP scope (real pool to the module-schema PG via the
in-network DSN) and ``FakeHttpClient`` at TRANSIENT scope (fresh instance
per invocation, deterministic, no sockets).

Failure-path semantics verified against the library, not guessed:

- ``enrich_order`` declares no ``retry=``, so the decorator applies the
  default ``RetryPolicy()`` (actor.py: kind="transient", max_attempts=3,
  backoff="exponential", base=5s, jitter=0.2).
- The actor's ``RuntimeError`` is retryable-transient: attempts 1-2 go
  through ``mark_retry`` (error recorded on the row, job rescheduled at
  ~5s / ~10s); attempt 3 exhausts the budget → ``RetryClassifier.classify``
  returns ``Fail(error_class=type(exc).__name__)`` → ``mark_failed``
  (retry.py, backend/_terminal.py). The consumer builds
  ``ErrorInfo(error_class=type(exc).__name__, error_message=str(exc))``
  (worker/_handlers.py), so the terminal row carries
  ``error_class='RuntimeError'`` /
  ``error_message='simulated enrichment fetch failure'``. Wall time to the
  terminal state ≈ 15-18s including jitter.
- Column names come from migrations (01.00.00_01_pre_initial.sql):
  ``jobs.error_class`` / ``error_message`` / ``error_traceback`` — there is
  no single ``error`` column. The same triple exists on ``job_attempts``.

Every test requests ``e2e_worker`` explicitly: the worker container fixture
is not autouse, so no worker (and no dispatch) exists unless a test pulls it
in.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest

from taskq import JobFailed

from ._assertions import fetch_effects
from .actors import EnrichOrderPayload, EnrichResult, enrich_order

if TYPE_CHECKING:
    import asyncpg

    from taskq import TaskQ

    from .conftest import E2ESchema, E2EWorker

pytestmark = [pytest.mark.e2e, pytest.mark.timeout(900)]


async def test_di_injected_clients_work_in_container(
    e2e_client: TaskQ,
    e2e_worker: E2EWorker,
    e2e_pg_pool: asyncpg.Pool,
    e2e_schema: E2ESchema,
    run_id: str,
) -> None:
    """DI providers resolve inside the container: typed result + both effects.

    The ``fetch`` effect is written through the LOOP-scope asyncpg pool
    after the TRANSIENT-scope ``FakeHttpClient`` answers ``GET
    /orders/ord-1/enrichment`` (static 200 payload); the ``enriched`` effect
    carries the fake's status. Effect rows are inserted sequentially by one
    job, so seq order is deterministic: fetch then enriched.
    """
    handle = await e2e_client.enqueue(
        enrich_order,
        EnrichOrderPayload(run_id=run_id, order_id="ord-1", fail_fetch=False),
    )

    result = await handle.wait(timeout=60)

    assert isinstance(result, EnrichResult)
    assert result.order_id == "ord-1"
    assert result.enriched is True

    rows = await fetch_effects(e2e_pg_pool, e2e_schema.schema_name, run_id)
    assert [row["kind"] for row in rows] == ["fetch", "enriched"]
    fetch_detail: dict[str, object] = json.loads(rows[0]["detail"])
    assert fetch_detail["run_id"] == run_id
    assert fetch_detail["order_id"] == "ord-1"
    assert fetch_detail["path"] == "/orders/ord-1/enrichment"
    enriched_detail: dict[str, object] = json.loads(rows[1]["detail"])
    assert enriched_detail["run_id"] == run_id
    assert enriched_detail["order_id"] == "ord-1"
    assert enriched_detail["status"] == 200


async def test_di_actor_failure_propagates(
    e2e_client: TaskQ,
    e2e_worker: E2EWorker,
    e2e_pg_pool: asyncpg.Pool,
    e2e_schema: E2ESchema,
    run_id: str,
) -> None:
    """fail_fetch=True → terminal ``failed`` with the actor error on the row.

    Asserts the client-visible contract (``JobFailed`` carrying the row) and
    the persisted ground truth (``jobs.error_class`` / ``error_message``;
    three per-attempt history rows, each ``RuntimeError``). The actor raises
    before any ``_record_effect`` call, so the run must leave zero effects —
    proof the failure happened inside the actor body, not in the harness.
    """
    handle = await e2e_client.enqueue(
        enrich_order,
        EnrichOrderPayload(run_id=run_id, order_id="ord-2", fail_fetch=True),
    )

    with pytest.raises(JobFailed) as exc_info:
        await handle.wait(timeout=90)

    failed_row = exc_info.value.row
    assert failed_row.status == "failed"
    assert failed_row.error_class == "RuntimeError"
    assert failed_row.error_message is not None
    assert "simulated enrichment fetch failure" in failed_row.error_message

    db_row = await e2e_pg_pool.fetchrow(
        f"""
        SELECT status, error_class, error_message
        FROM "{e2e_schema.schema_name}".jobs
        WHERE id = $1
        """,
        handle.job_id,
    )
    assert db_row is not None
    assert db_row["status"] == "failed"
    assert db_row["error_class"] == "RuntimeError"
    assert db_row["error_message"] is not None
    assert "simulated enrichment fetch failure" in db_row["error_message"]

    attempt_rows = await e2e_pg_pool.fetch(
        f"""
        SELECT attempt, outcome, error_class
        FROM "{e2e_schema.schema_name}".job_attempts
        WHERE job_id = $1
        ORDER BY attempt
        """,
        handle.job_id,
    )
    assert [row["attempt"] for row in attempt_rows] == [1, 2, 3]
    assert {row["outcome"] for row in attempt_rows} == {"failed"}
    assert {row["error_class"] for row in attempt_rows} == {"RuntimeError"}

    assert await fetch_effects(e2e_pg_pool, e2e_schema.schema_name, run_id) == []

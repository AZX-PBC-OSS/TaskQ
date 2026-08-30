"""Dispatch must write its state-change events in one statement.

Dispatch previously looped `for rec in records: await conn.execute(...)` -- one
awaited round trip per dispatched job, issued INSIDE the transaction still
holding the `FOR UPDATE SKIP LOCKED` row locks. At a batch of 50 against a
managed Postgres (~1-3ms RTT) that is 50-150ms of extra transaction hold per
dispatch cycle, per worker. Correctness was never in question -- a failure rolls
the whole batch back to `pending` -- but lock hold time directly narrows the
window other dispatchers can work in, and this is the hot path.

Every row in one batch shares `kind` and `detail` (same from_state, to_state and
worker_id), so only the job ids vary and a single `unnest` over a `uuid[]`
suffices.
"""

from __future__ import annotations

import re
from uuid import uuid4

import pytest

from taskq.backend._sql import INSERT_EVENTS_BATCH_SQL
from taskq.backend._sql_templates import render


def test_batch_sql_is_a_single_multi_row_insert() -> None:
    sql = INSERT_EVENTS_BATCH_SQL.format(schema="taskq")
    assert sql.count("INSERT INTO") == 1
    assert "unnest($1::uuid[])" in sql
    # clock_timestamp() must stay per-row, matching the statement it replaces.
    assert "clock_timestamp()" in sql


def test_rendered_templates_expose_the_batch_form() -> None:
    tmpl = render("taskq")
    assert "unnest" in tmpl.insert_events_batch
    # The single-row form is still used by other call sites (sweeps, terminal
    # writes), so it must not have been removed.
    assert "VALUES ($1, clock_timestamp(), $2, $3::jsonb)" in tmpl.insert_event


def test_dispatch_uses_the_batch_template_and_no_per_row_loop() -> None:
    import inspect

    from taskq.backend import _dispatch

    src = inspect.getsource(_dispatch._dispatch_batch)
    assert "sql.insert_events_batch" in src
    assert not re.search(r"for rec in records:\s*\n\s+await conn\.execute", src), (
        "per-row event INSERT loop is back"
    )


def test_schema_is_still_validated_in_the_batch_template() -> None:
    with pytest.raises(ValueError, match=r"[Ii]nvalid"):
        render('evil"; DROP SCHEMA public CASCADE; --')


@pytest.mark.integration
async def test_events_land_once_per_dispatched_job(clean_pg_conn, module_pg_schema) -> None:  # type: ignore[no-untyped-def]  # Why: pytest fixtures.
    """Behavioural equivalence: the batch statement writes exactly the rows the
    loop did, with the same kind and detail."""
    schema = module_pg_schema.schema_name
    tmpl = render(schema)

    job_ids = [uuid4() for _ in range(5)]
    await clean_pg_conn.execute(
        f'INSERT INTO "{schema}".jobs '  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
        "(id, actor, queue, payload, status, max_attempts, retry_kind) "
        "SELECT id, 'a', 'default', '{}'::jsonb, 'pending', 3, 'transient' "
        "FROM unnest($1::uuid[]) AS t(id)",
        job_ids,
    )

    detail = '{"from_state": "pending", "to_state": "running", "worker_id": "w1"}'
    await clean_pg_conn.execute(tmpl.insert_events_batch, job_ids, "state_change", detail)

    rows = await clean_pg_conn.fetch(
        f'SELECT job_id, kind, detail FROM "{schema}".job_events ORDER BY occurred_at'  # noqa: S608  # Why: schema is a test-fixture identifier.
    )
    assert len(rows) == len(job_ids)
    assert {r["job_id"] for r in rows} == set(job_ids)
    assert {r["kind"] for r in rows} == {"state_change"}


@pytest.mark.integration
async def test_empty_batch_writes_nothing(clean_pg_conn, module_pg_schema) -> None:  # type: ignore[no-untyped-def]  # Why: pytest fixtures.
    """Dispatch guards on `if records:`, but the statement itself must also be
    a no-op on an empty array rather than erroring."""
    schema = module_pg_schema.schema_name
    tmpl = render(schema)
    await clean_pg_conn.execute(tmpl.insert_events_batch, [], "state_change", "{}")
    count = await clean_pg_conn.fetchval(f'SELECT count(*) FROM "{schema}".job_events')  # noqa: S608  # Why: schema is a test-fixture identifier.
    assert count == 0

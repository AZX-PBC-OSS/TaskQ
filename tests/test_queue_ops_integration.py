"""`queues` rows must be reachable without hand-written SQL.

The defect: round-robin fairness engages only when a `{schema}.queues` row says
`mode = 'round_robin'`, but nothing in TaskQ ever inserted such a row -- no
migration seed, no bootstrap step, no CLI. So a job's `fairness_key` was
accepted, persisted, carried through the dispatch CTE, and discarded. Worse, the
remedy both guides gave was `UPDATE ... WHERE name = 'x'`, which affects **zero
rows** when the row does not exist, so configuring fairness appeared to succeed
and silently did nothing.

These run against real Postgres because the UPSERT-vs-UPDATE distinction is the
whole point and cannot be shown against a mock.
"""

from __future__ import annotations

import asyncpg
import pytest

from taskq.testing.fixtures import ModulePgSchema
from taskq.worker.queue_ops import (
    DEFAULT_QUEUE_MODE,
    get_queue,
    list_queues,
    set_queue_max_concurrent,
    set_queue_mode,
)

pytestmark = pytest.mark.integration


async def test_plain_update_is_a_silent_noop_on_a_fresh_deployment(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """The exact trap the old docs walked operators into.

    This is the behaviour that made the bug so hard to see: the documented
    command succeeds, reports no error, and changes nothing.
    """
    status = await clean_pg_conn.execute(
        f'UPDATE "{module_pg_schema.schema_name}".queues '  # noqa: S608  # Why: reproducing the exact raw SQL the old docs told operators to run; schema is a test-fixture identifier.
        "SET mode = 'round_robin' WHERE name = 'fresh'"
    )
    assert status == "UPDATE 0"
    assert await get_queue(clean_pg_conn, "fresh", schema=module_pg_schema.schema_name) is None


async def test_set_queue_mode_upserts_so_it_works_on_a_fresh_deployment(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    row = await set_queue_mode(
        clean_pg_conn, "tenants", "round_robin", schema=module_pg_schema.schema_name
    )
    assert (row.name, row.mode) == ("tenants", "round_robin")

    stored = await get_queue(clean_pg_conn, "tenants", schema=module_pg_schema.schema_name)
    assert stored is not None
    assert stored.mode == "round_robin"


async def test_set_queue_mode_is_idempotent_and_updates_an_existing_row(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    await set_queue_mode(clean_pg_conn, "q", "round_robin", schema=module_pg_schema.schema_name)
    again = await set_queue_mode(
        clean_pg_conn, "q", "round_robin", schema=module_pg_schema.schema_name
    )
    assert again.mode == "round_robin"

    back = await set_queue_mode(
        clean_pg_conn, "q", "strict_fifo", schema=module_pg_schema.schema_name
    )
    assert back.mode == "strict_fifo"

    rows = [
        r
        for r in await list_queues(clean_pg_conn, schema=module_pg_schema.schema_name)
        if r.name == "q"
    ]
    assert len(rows) == 1, "upsert must not duplicate the row"


async def test_absent_row_means_defaults_not_missing_queue(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """`None` means "runs on defaults", and the default is the one that
    makes fairness_key inert."""
    assert (
        await get_queue(clean_pg_conn, "never-configured", schema=module_pg_schema.schema_name)
        is None
    )
    assert DEFAULT_QUEUE_MODE == "strict_fifo"


async def test_set_max_concurrent_upserts_and_clears(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    row = await set_queue_max_concurrent(
        clean_pg_conn, "capped", 4, schema=module_pg_schema.schema_name
    )
    assert row.max_concurrent == 4
    # Creating via the max_concurrent path must not disturb the mode default.
    assert row.mode == "strict_fifo"

    cleared = await set_queue_max_concurrent(
        clean_pg_conn, "capped", None, schema=module_pg_schema.schema_name
    )
    assert cleared.max_concurrent is None


async def test_setting_mode_preserves_max_concurrent_and_vice_versa(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """The two upserts touch one column each; neither may clobber the other."""
    await set_queue_max_concurrent(clean_pg_conn, "both", 7, schema=module_pg_schema.schema_name)
    after_mode = await set_queue_mode(
        clean_pg_conn, "both", "round_robin", schema=module_pg_schema.schema_name
    )
    assert after_mode.max_concurrent == 7

    after_cap = await set_queue_max_concurrent(
        clean_pg_conn, "both", 9, schema=module_pg_schema.schema_name
    )
    assert after_cap.mode == "round_robin"


async def test_invalid_mode_is_rejected_before_reaching_the_check_constraint(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    with pytest.raises(ValueError, match="invalid queue mode"):
        await set_queue_mode(clean_pg_conn, "q", "fifo", schema=module_pg_schema.schema_name)


async def test_invalid_schema_is_rejected(
    clean_pg_conn: asyncpg.Connection, module_pg_schema: ModulePgSchema
) -> None:
    with pytest.raises(ValueError, match="invalid schema name"):
        await list_queues(clean_pg_conn, schema='evil"; DROP SCHEMA public CASCADE; --')

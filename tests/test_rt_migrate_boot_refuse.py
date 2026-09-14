# Why: schema is a fixed test identifier, not user input; every value is $-bound.
"""Red-team: worker boot ordering vs migration currency (rolling-deploy window).

The worker never applies migrations by design, and the only schema-currency
guard on the whole boot path is the queue-cap query's UndefinedColumnError →
RuntimeError, which covers exactly one migration (01.00.04's
``queues.max_concurrent``). A schema stopped one release earlier — 01.00.07,
missing 01.00.08's ``snooze_count`` / ``rate_limit_blocked_count`` — passes
every boot step: the enqueue INSERT's column list omits the counters, and the
dispatch claim's ``RETURNING j.*`` record then dies in
``_job_row_from_record``'s strict ``rec["snooze_count"]`` (a bare KeyError)
AFTER the claim has already committed the job to running+locked — every
dispatched job loops through lock-expiry crash-reclaim and never executes.

Desired observable, per the boot path's own doctrine ("a deployment mistake
that must crash startup loudly, not a best-effort condition to warn about"):
pending migrations must abort boot before dispatch starts.
"""

from __future__ import annotations

import asyncio
import contextlib

import asyncpg
import pytest

from taskq._ids import new_base62
from taskq.migrate import apply_pending, list_applied
from taskq.settings import WorkerSettings
from taskq.testing.health import unique_health_sock_path
from taskq.worker.run import _main

pytestmark = pytest.mark.integration

_DENIAL_COUNTERS_KEY = "01.00.08_01:pre"


async def _drop_schema(pg_dsn: str, schema: str) -> None:
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    finally:
        await conn.close()


async def _boot_until_idle_or_raise(
    settings: WorkerSettings,
) -> tuple[int | None, BaseException | None]:
    """Boot the real ``_main`` in drain mode; return (exit code, exception).

    Drain mode bounds the run: an empty queue settles within the idle window
    and the worker exits on its own, so a boot that does NOT refuse completes
    quickly instead of hanging the test.
    """
    task = asyncio.create_task(
        _main(
            settings,
            until_idle=True,
            idle_settle_window=0.1,
            idle_poll_interval=0.1,
            idle_max_runtime=8.0,
        )
    )
    deadline = asyncio.get_running_loop().time() + 45.0
    while asyncio.get_running_loop().time() < deadline:
        if task.done():
            break
        await asyncio.sleep(0.05)
    if not task.done():
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        pytest.fail("worker boot did not reach a decision within 45s — unbounded boot path")
    exc = task.exception()
    if exc is not None:
        return None, exc
    return task.result(), None


async def test_worker_boot_refuses_while_denial_counter_migration_is_pending(
    pg_dsn: str,
) -> None:
    """RED contract: boot must refuse while migrations are pending.

    The queue-cap guard refuses a schema missing 01.00.04; the same doctrine
    must cover ANY pending migration — a schema missing the newest one boots
    today and half-works (claims commit, then every job dies on the strict
    ``rec["snooze_count"]`` row conversion).
    """
    schema = f"tmg_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema, target="01.00.07_01")
        assert _DENIAL_COUNTERS_KEY not in await list_applied(conn, schema), (
            "precondition: schema must be one release behind (01.00.08 pending)"
        )
    finally:
        await conn.close()

    settings = WorkerSettings.load_from_dict(
        {
            "pg_dsn": pg_dsn,
            "schema_name": schema,
            "health_socket_path": unique_health_sock_path("rt_migrate_boot"),
        }
    )
    try:
        result, raised = await _boot_until_idle_or_raise(settings)
        assert raised is not None and "migrat" in str(raised).lower(), (
            "contract: the worker must REFUSE to boot while migrations are pending "
            f"(schema at 01.00.07, {_DENIAL_COUNTERS_KEY} unapplied) — instead boot "
            f"completed and served against the stale schema (result={result!r}, "
            f"raised={raised!r}); the only boot-time guard covers 01.00.04 alone, "
            "and every claimed job then dies post-commit on the strict "
            "rec['snooze_count'] read in _job_row_from_record"
        )
    finally:
        await _drop_schema(pg_dsn, schema)


async def test_worker_boot_drains_cleanly_on_current_schema(pg_dsn: str) -> None:
    """Control for the refusal contract: the identical boot shape against a
    CURRENT schema must boot, idle out, and exit 0 — proving the refusal
    test's outcome is about schema staleness, not a broken in-process boot."""
    schema = f"tmg_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema)
        assert _DENIAL_COUNTERS_KEY in await list_applied(conn, schema), (
            "precondition: schema must be fully migrated"
        )
    finally:
        await conn.close()

    settings = WorkerSettings.load_from_dict(
        {
            "pg_dsn": pg_dsn,
            "schema_name": schema,
            "health_socket_path": unique_health_sock_path("rt_migrate_boot"),
        }
    )
    try:
        result, raised = await _boot_until_idle_or_raise(settings)
        assert raised is None and result == 0, (
            "control contract: the same boot against a current schema must drain "
            f"and exit 0 (result={result!r}, raised={raised!r})"
        )
    finally:
        await _drop_schema(pg_dsn, schema)

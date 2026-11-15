# ruff: noqa: ASYNC109  # Why: the transparent low-level observer mirrors asyncpg's native fetchval(timeout=...) surface.
"""Private two-process worker entry for the P2 real-saturation proof.

The production bootstrap and native asyncpg connections remain in use.  A
narrow method observer records each actual ``EMIT_IN_FLIGHT_SQL`` read in its
own committed witness row; it does not replace a pool, result, claim, provider,
or transaction.  The independent witness insert survives a subsequent emit
rollback, so refused locked admissions cannot disappear from the evidence.
"""

from __future__ import annotations

import asyncio
import contextlib
import faulthandler
import os
import signal
import sys
from collections.abc import Awaitable, Callable

import asyncpg
from examples.adoption import paged_sync as sample

from taskq.settings import WorkerSettings
from taskq.worker.run import _main

_ORIGINAL_FETCHVAL: Callable[..., Awaitable[object]] = asyncpg.Connection.fetchval


# Why: this transparent observer mirrors the native asyncpg timeout parameter.
async def _observed_fetchval(
    conn: asyncpg.Connection,
    query: str,
    *args: object,
    column: int = 0,
    timeout: float | None = None,
) -> object:
    result = await _ORIGINAL_FETCHVAL(conn, query, *args, column=column, timeout=timeout)
    if (
        "Only the exact committed park representation is not executable work." in query
        and "AND id <> $2::uuid" in query
        and len(args) >= 2
    ):
        # The schema is validated by WorkerSettings before this process serves
        # work.  A separate native-bound read makes the admission's phase visible
        # even when the engine's enclosing emit transaction later rolls back.
        schema = os.environ["TASKQ_SCHEMA_NAME"]
        source_status = await _ORIGINAL_FETCHVAL(
            conn,
            f'SELECT status::text FROM "{schema}".jobs WHERE id = $1',  # noqa: S608  # settings-validated identifier only
            args[1],
            column=0,
        )
        witness = await asyncpg.connect(os.environ["TASKQ_PG_DSN"])
        try:
            await witness.execute(
                f'INSERT INTO "{schema}".adoption_sync_admission_reads '  # noqa: S608  # settings-validated identifier only
                "(scenario, pid, flow_id, source_id, outstanding, source_status, in_transaction) "
                "VALUES ($1, $2, $3, $4, $5, $6, $7)",
                os.environ["ADOPTION_SYNC_SCENARIO"],
                os.getpid(),
                args[0],
                args[1],
                int(result or 0),
                source_status,
                conn.is_in_transaction(),
            )
        finally:
            await witness.close()
    return result


async def _run() -> int:
    # Instrument only in these owned proof subprocesses.  Production and every
    # other system worker retain the untouched method.
    asyncpg.Connection.fetchval = _observed_fetchval  # type: ignore[method-assign]  # Why: process-local transparent observation only.
    settings = WorkerSettings.load()
    return await _main(
        settings,
        actor_registry={
            "adoption_source_probe": sample.source_probe,
            "adoption_record_probe": sample.leaf_probe,
            "adoption_sibling_probe": sample.sibling_probe,
        },
    )


if __name__ == "__main__":
    faulthandler.register(signal.SIGUSR2, all_threads=True)
    try:
        with asyncio.Runner() as runner:
            code = runner.run(_run())
        with contextlib.suppress(ValueError):  # Why: retain the normal post-drain verdict.
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
        sys.exit(code)
    except KeyboardInterrupt:
        sys.exit(0)

"""Manual validation harness for dedicated-connection TCP keepalive (§6.6, §12.5).

Verifies that ``notify_conn`` and ``leader_conn`` (the dedicated, non-pooled
asyncpg connections opened by ``taskq.worker.deps.open_worker_deps``) detect a
TCP-level partition within the keepalive window
(``TCP_KEEPIDLE + TCP_KEEPCNT * TCP_KEEPINTVL = 30 + 3 * 5 = 45 s``) by pausing
the PG container mid-session.

Excluded from CI because the wait window is ~46 s. Run manually on a workstation
with Docker available::

    uv run python validation/tcp_keepalive_partition.py

Expected outcome: both connections raise (OSError or
asyncpg.PostgresConnectionError) on the next operation; the script prints the
elapsed detection time for each connection and exits 0. On regression the
script either hangs past the deadline (no keepalive applied) or both
connections continue answering (PG never paused — Docker permission issue).
"""

import asyncio
import time

import asyncpg
from testcontainers.postgres import PostgresContainer

from taskq.settings import WorkerSettings
from taskq.worker.deps import open_worker_deps

DEADLINE_SECONDS = 60.0  # 45 s keepalive window + 15 s margin


async def _probe(conn: asyncpg.Connection, label: str, deadline: float) -> float:
    """Poll ``SELECT 1`` until the connection raises; return elapsed seconds."""
    start = time.monotonic()
    while True:
        elapsed = time.monotonic() - start
        if elapsed > deadline:
            raise TimeoutError(
                f"{label}: no failure within {deadline:.0f}s — keepalive likely not applied"
            )
        try:
            await conn.fetchval("SELECT 1")
        except (OSError, asyncpg.PostgresConnectionError, asyncpg.InterfaceError):
            return elapsed
        await asyncio.sleep(1.0)


async def run() -> None:
    print("Starting PG18 testcontainer...")
    with PostgresContainer(
        image="postgres:18-alpine",
        username="taskq",
        password="taskq",  # noqa: S106  # Why: ephemeral test-container credential
        dbname="taskq",
    ) as container:
        dsn = container.get_connection_url().replace("postgresql+psycopg2://", "postgresql://")
        settings = WorkerSettings.load_from_dict({"TASKQ_PG_DSN": dsn})

        async with open_worker_deps(settings) as deps:
            print("WorkerDeps open. Pausing PG container to simulate partition...")
            container.get_wrapped_container().pause()

            try:
                results = await asyncio.gather(
                    _probe(deps.notify_conn, "notify_conn", DEADLINE_SECONDS),
                    _probe(deps.leader_conn, "leader_conn", DEADLINE_SECONDS),
                )
            finally:
                container.get_wrapped_container().unpause()

        notify_elapsed, leader_elapsed = results
        print(f"notify_conn detected partition in {notify_elapsed:.1f}s")
        print(f"leader_conn detected partition in {leader_elapsed:.1f}s")

        # Sanity bound: keepalive window is ~45s; allow up to 60s.
        assert notify_elapsed < DEADLINE_SECONDS, "notify_conn too slow"
        assert leader_elapsed < DEADLINE_SECONDS, "leader_conn too slow"
        print("PASS")


if __name__ == "__main__":
    asyncio.run(run())

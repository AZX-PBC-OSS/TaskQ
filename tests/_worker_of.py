"""A worker id that exists in a backend's ``workers`` table.

Six parity suites (the reclaim/retry-budget/cancel-state parity files and
the unique-for window suite) grew byte-identical copies of this helper,
each claiming to "mirror" another copy in its Why comments — the drift the
shared module ends. The harness speaks to BOTH backends through the same
contract the differential corpus pins: the in-memory side reads the
backend's canonical worker id, the PG side inserts a row the way a real
worker's registration would.
"""

from __future__ import annotations

from uuid import UUID

from taskq._ids import new_uuid
from taskq.backend._protocol import Backend
from taskq.backend.postgres import PostgresBackend
from taskq.testing.in_memory import InMemoryBackend

__all__ = ["worker_of"]


async def worker_of(backend: Backend) -> UUID:
    """A worker id that exists in the backend's ``workers`` table."""
    if isinstance(backend, InMemoryBackend):
        return backend._worker_id  # pyright: ignore[reportPrivateUsage]  # Why: canonical worker identity for InMemoryBackend; the private read is the twin of the INSERT the PG path performs.
    assert isinstance(backend, PostgresBackend)
    schema: str = backend._schema_name  # pyright: ignore[reportPrivateUsage]  # Why: the parity suites construct the backend from the module fixture, so the schema is fixture-derived; the private read mirrors what the dispatch path itself does.
    pool = backend._worker_pool  # pyright: ignore[reportPrivateUsage]  # Why: same fixture-derived backend.
    worker_id = new_uuid()
    async with pool.acquire() as conn:  # pyright: ignore[reportUnknownVariableType]  # Why: asyncpg stubs yield PoolConnectionProxy | Unknown
        await conn.execute(
            f'INSERT INTO "{schema}".workers (id, hostname, pid, queues) VALUES ($1, $2, $3, $4)',  # noqa: S608 # Why: schema is fixture-derived and _IDENT_RE-validated; every value is $N-bound
            worker_id,
            "test-host",
            12345,
            ["default"],
        )
    return worker_id

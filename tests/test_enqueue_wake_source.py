"""The ``tr_notify_job_insert`` trigger is the sole wake source for inserts.

Every enqueue path used to follow its INSERT with its own
``SELECT pg_notify(wake_channel, '')`` - a statement the trigger already
issues for every pending row, on the same channel with the same empty
payload. Inside one transaction Postgres coalesces the pair, so on the pool
path the app-side statement was pure cost; on a caller's bare connection
the INSERT and the notify were two transactions and every listener was
woken twice per enqueue; and the app-side statement was gated on "a row was
inserted", not on the row being dispatchable, so a future-dated enqueue
woke the whole fleet for nothing. The trigger's WHEN clause gates the
notify on the row being due (every insert path decides ``status``
server-side, so ``pending`` means
dispatchable now).

Each assertion counts deliveries over a settling window rather than
waiting for the first: the property under test is the exact number.
"""

import asyncio
from datetime import timedelta

import asyncpg
import pytest

from taskq.constants import wake_channel
from taskq.testing.fixtures import JobsApp
from taskq.testing.jobs import make_enqueue_args

pytestmark = pytest.mark.integration

#: The positive cases deliver in milliseconds; a spurious second delivery
#: would too, so this window is decisive for the counts below.
_SETTLE = 0.5


class _Listener:
    def __init__(self, dsn: str, channel: str) -> None:
        self._dsn = dsn
        self._channel = channel
        self.deliveries = 0

    async def __aenter__(self) -> "_Listener":
        self._conn = await asyncpg.connect(self._dsn)
        await self._conn.add_listener(self._channel, self._on_notify)  # pyright: ignore[reportArgumentType]  # Why: asyncpg stubs over-narrow the callback type.
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self._conn.close()

    def _on_notify(self, _conn: object, _pid: int, _ch: str, _payload: str) -> None:
        self.deliveries += 1

    async def settled(self) -> int:
        await asyncio.sleep(_SETTLE)
        return self.deliveries


def _listener(app: JobsApp) -> _Listener:
    return _Listener(str(app.deps.settings.pg_dsn), wake_channel(app.deps.settings.schema_name))


async def test_a_pool_enqueue_wakes_listeners_exactly_once(clean_jobs_app: JobsApp) -> None:
    async with _listener(clean_jobs_app) as listener:
        await clean_jobs_app.backend.enqueue(make_enqueue_args())
        assert await listener.settled() == 1


async def test_an_enqueue_on_a_bare_caller_connection_wakes_exactly_once(
    clean_jobs_app: JobsApp,
) -> None:
    """No transaction to coalesce into: only the trigger's delivery is sent."""
    async with _listener(clean_jobs_app) as listener:
        pool = clean_jobs_app.backend._worker_pool  # pyright: ignore[reportPrivateUsage]  # Why: the bare-connection path is reached only through a caller-supplied conn.
        async with pool.acquire() as conn:  # pyright: ignore[reportUnknownVariableType]  # Why: asyncpg stubs yield PoolConnectionProxy | Unknown
            assert not conn.is_in_transaction()
            await clean_jobs_app.backend.enqueue_with_conn(conn, make_enqueue_args())
        assert await listener.settled() == 1


async def test_a_future_dated_enqueue_wakes_nobody(clean_jobs_app: JobsApp) -> None:
    """A row that lands as ``scheduled`` is not dispatchable; waking every
    worker for it is the thundering herd the trigger's WHEN clause exists
    to prevent."""
    async with _listener(clean_jobs_app) as listener:
        later = clean_jobs_app.backend._clock.now() + timedelta(hours=1)  # pyright: ignore[reportPrivateUsage]  # Why: the backend's own clock keeps the stamp in the store's domain.
        row = await clean_jobs_app.backend.enqueue(make_enqueue_args(scheduled_at=later))
        assert row.status == "scheduled"
        assert await listener.settled() == 0


async def test_a_batch_enqueue_wakes_listeners_exactly_once(clean_jobs_app: JobsApp) -> None:
    async with _listener(clean_jobs_app) as listener:
        await clean_jobs_app.backend.enqueue_batch([make_enqueue_args() for _ in range(5)])
        assert await listener.settled() == 1


async def test_a_copy_enqueue_wakes_listeners_exactly_once(clean_jobs_app: JobsApp) -> None:
    async with _listener(clean_jobs_app) as listener:
        await clean_jobs_app.backend.enqueue_batch_fast([make_enqueue_args() for _ in range(5)])
        assert await listener.settled() == 1


async def test_a_future_dated_copy_batch_wakes_nobody(clean_jobs_app: JobsApp) -> None:
    """The COPY tier decides status in its fixup UPDATE, after the rows
    are in: rows that land ``pending`` at COPY time and are flipped to
    ``scheduled`` afterwards would fire the INSERT trigger for work
    nobody can dispatch - the herd the trigger's gate exists to prevent."""
    async with _listener(clean_jobs_app) as listener:
        later = clean_jobs_app.backend._clock.now() + timedelta(hours=1)  # pyright: ignore[reportPrivateUsage]  # Why: the backend's own clock keeps the stamp in the store's domain.
        await clean_jobs_app.backend.enqueue_batch_fast(
            [make_enqueue_args(scheduled_at=later) for _ in range(5)]
        )
        assert await listener.settled() == 0


async def test_a_mixed_copy_batch_wakes_listeners_exactly_once(clean_jobs_app: JobsApp) -> None:
    async with _listener(clean_jobs_app) as listener:
        later = clean_jobs_app.backend._clock.now() + timedelta(hours=1)  # pyright: ignore[reportPrivateUsage]  # Why: the backend's own clock keeps the stamp in the store's domain.
        await clean_jobs_app.backend.enqueue_batch_fast(
            [make_enqueue_args(scheduled_at=later) for _ in range(3)]
            + [make_enqueue_args() for _ in range(2)]
        )
        assert await listener.settled() == 1


async def test_a_copy_batch_on_a_bare_caller_connection_wakes_exactly_once(
    clean_jobs_app: JobsApp,
) -> None:
    async with _listener(clean_jobs_app) as listener:
        pool = clean_jobs_app.backend._worker_pool  # pyright: ignore[reportPrivateUsage]  # Why: the bare-connection path is reached only through a caller-supplied conn.
        async with pool.acquire() as conn:  # pyright: ignore[reportUnknownVariableType]  # Why: asyncpg stubs yield PoolConnectionProxy | Unknown
            assert not conn.is_in_transaction()
            await clean_jobs_app.backend.enqueue_batch_fast(
                [make_enqueue_args() for _ in range(5)], connection=conn
            )
        assert await listener.settled() == 1

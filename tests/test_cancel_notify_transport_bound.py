"""THE CANCEL NOTIFY TRANSPORT'S BOUND — the system-e2e PG17 lane's convictions.

The CI conviction (run 38053606389, the ``system-e2e (17)`` leg at
f7155214; PG15/16/18 green on the same commit): one silently-dead PG
socket under the runner's weather converted ONE infrastructure event
into THREE legs of the matrix:

1. (leg F) ``test_operator_cancel_racing_the_sigterm_owns_the_exit`` —
   ``Timeout (>240.0s)``. The test's own captured stdout ends at the
   enqueues: every await in its front half is bounded (the
   ``_wait_running`` cap, the phase poll, the harness's stop/kill
   bounds) EXCEPT the operator's ``cancel_where`` and the ledger reads
   — and the cancel row writes had committed (the zombie rows read
   ``cancel_phase=1`` downstream), so the await that never returned was
   the NOTIFY TRANSPORT leg that follows the committed drain
   (``cancel_where_notify``): a plain ``conn.execute`` of
   ``pg_notify`` with no client-side bound, hung forever on a socket
   the server had stopped answering.
2. (leg E) the teardown error — the test task still pending at call
   end: the test's own ``finally`` hung the same way (``delete_tagged``
   on the same dead ledger socket, unbounded), so the reaped workers'
   population (tagged, operator-cancel-requested rows) was never
   deleted and the test coroutine never finished.
3. (leg F2) the sibling scenario — ``ended 'cancelled'
   (archived=False)``: the zombie rows share the module schema AND the
   ``sys-req`` tag; the successor's leader sweep reclaimed their
   expired leases and terminalised them with the operator's OWN
   recorded verdict (``cancelled``), and the sibling's per-row assert
   read one of the ZOMBIE's rows, not its own.

The root defect class: **the NOTIFY transport legs and the tier's
ledger cleanup are unbounded client-side**. The law this lane lands
(the same one the HITL listener already declares): THE ROWS ARE THE
TRUTH, THE NOTIFY IS TRANSPORT — a transport that never answers must
not hold the operator's call, the leader's sweep tick, or the tier's
own cleanup hostage. The three pins below replay each convicted leg
deterministically; the cures (the transport bound in
``taskq.backend.postgres``, the bounded ledger reads and the
resilient cleanup in ``tests/system_e2e``) turn each red green.

Pin 1 is the house's structural-stand-in style
(``tests/test_connections.py``'s ``_FakePool``): a wrapper pool that
serves the row-write phase from a REAL pool against REAL PostgreSQL,
then hands the NOTIFY phase a connection whose ``execute`` never
answers — the exact shape the dead socket produced. Pins 2 and 3
replay the tier's cleanup halves (the never-answering ledger read; the
population that must still be deleted through a fresh connection after
the ledger connection died).
"""

from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING, Any
from uuid import UUID

import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import JobId
from taskq.testing.assertions import wait_for_condition
from taskq.testing.pg import create_running_job

if TYPE_CHECKING:
    import asyncpg

    from taskq.backend.postgres import PostgresBackend
    from taskq.testing.fixtures import ModulePgSchema

pytestmark = pytest.mark.integration

#: The transport bound the cure installs in ``taskq.backend.postgres``.
#: The pins' outer budgets are sized to outlive it with headroom.
_TRANSPORT_BOUND_S = 5.0
_PIN_WAIT_S = 60.0

_TAG = "notify-bound"


# ── the stand-ins (the house's _FakePool style) ────────────────────────


class _HangingConn:
    """A connection whose ``execute`` never answers: the silently-dead
    socket (packets accepted, never delivered), the shape the CI leg's
    black-holed NOTIFY transport produced."""

    async def execute(self, *args: object, **kwargs: object) -> str:
        await asyncio.Event().wait()  # never
        return ""  # pragma: no cover - unreachable

    async def fetch(self, *args: object, **kwargs: object) -> list[Any]:
        await asyncio.Event().wait()  # never
        return []  # pragma: no cover - unreachable

    async def fetchrow(self, *args: object, **kwargs: object) -> None:
        await asyncio.Event().wait()  # never


class _HangingAcquire:
    """The awaitable half of a pool ``acquire()`` that hands out the
    hanging connection (``_bounded_checkout`` awaits ``pool.acquire()``
    when the result is awaitable — asyncpg's documented dual surface)."""

    def __await__(self) -> Any:
        async def _hand_out() -> _HangingConn:
            return _HangingConn()

        return _hand_out().__await__()


class _RealThenHangingPool:
    """Wrapper pool: the first *real_acquires* acquires delegate to the
    REAL pool (the row-write phase runs against REAL PostgreSQL and
    COMMITS); every later acquire — the NOTIFY transport's own checkout
    — hands out the hanging connection."""

    def __init__(self, real: Any, *, real_acquires: int) -> None:
        self._real = real
        self._real_acquires = real_acquires
        self._served = 0
        self.hanging_released = 0

    def acquire(
        self, timeout: float | None = None
    ) -> Any:  # Why: models asyncpg's acquire(timeout=...) signature verbatim - the stand-in doubles the documented dual surface, not a caller's budget knob.
        if self._served < self._real_acquires:
            self._served += 1
            # asyncpg's acquire() is documented as BOTH awaitable and an
            # async context manager; the await form is the one
            # _bounded_checkout takes, and the timeout kwarg passes
            # through verbatim either way.
            return (
                self._real.acquire(timeout=timeout) if timeout is not None else self._real.acquire()
            )
        return _HangingAcquire()

    async def release(self, conn: Any, *, timeout: float | None = None) -> None:  # noqa: ASYNC109  # Why: models asyncpg's release(timeout=...) signature verbatim - the stand-in doubles the documented surface, not a caller's budget knob.
        # The hanging connection's release is pool hygiene on a conn
        # that never answered: record it, never raise (the same
        # never-raises law _bounded_checkout imposes on its callers).
        if isinstance(conn, _HangingConn):
            self.hanging_released += 1
        else:
            await self._real.release(conn, timeout=timeout)  # type: ignore[reportUnknownMemberType]

    def __getattr__(self, name: str) -> Any:
        return getattr(self._real, name)


# ── pin 1: the operator's cancel owns its own return ───────────────────


@pytest.mark.timeout(180)
async def test_write_cancel_request_returns_when_the_notify_transport_never_answers(
    module_pg_schema: ModulePgSchema,
    clean_jobs_app: Any,
) -> None:
    """The convicted await (leg F): the cancel request's row write
    commits, then the NOTIFY transport's socket never answers — the
    call must still RETURN (the row IS the request; the heartbeat poll
    remains authoritative for signal delivery), inside its own bound,
    with the transport failure swallowed-and-warned. UNCURED the call
    hangs forever (the CI leg's 240s timeout); CURED the transport
    bound fires and the call returns the committed truth."""
    import asyncpg

    schema = module_pg_schema.schema_name
    backend: PostgresBackend = clean_jobs_app.backend
    conn: asyncpg.Connection = await asyncpg.connect(module_pg_schema.pg_dsn)
    try:
        job_id = await create_running_job(conn, schema, new_uuid())

        # The row-write phase (acquire #1) runs against the REAL pool;
        # the NOTIFY transport's checkout (acquire #2) never answers.
        # The backend's pool accessor is a property over the deps' pool
        # (the hot-reload seam), so the wrapper rides the deps.
        real_pool = backend._worker_pool  # pyright: ignore[reportPrivateUsage,reportAttributeAccessIssue]  # Why: the pin drives the production backend's documented pool seam; the underscore is the same private-access the _FakePool pins take.
        clean_jobs_app.deps.worker_pool = _RealThenHangingPool(real_pool, real_acquires=1)  # type: ignore[reportAttributeAccessIssue]  # Why: WorkerDeps' pool field is the seam the backend's property reads; the test double replaces it per the _FakePool precedent.
        started = time.monotonic()
        try:
            committed = await asyncio.wait_for(
                backend.write_cancel_request(JobId(job_id), "offboard"),
                timeout=_PIN_WAIT_S,
            )
        finally:
            clean_jobs_app.deps.worker_pool = real_pool  # type: ignore[reportAttributeAccessIssue]  # Why: the fixture's teardown closes the REAL pool; the wrapper must not survive the test.
        elapsed = time.monotonic() - started

        assert committed is True, "the row write committed - the call must report it"
        assert elapsed < _TRANSPORT_BOUND_S + 20.0, (
            f"the call returned at {elapsed:.1f}s - the transport bound did not fire"
        )
    finally:
        await conn.close()


# ── pin 2: the tier's settle read fails DESIGNED, never hangs ──────────


@pytest.mark.timeout(180)
async def test_wait_running_fails_designed_when_the_ledger_never_answers() -> None:
    """The hang class under the tier's own settle reads (the front half
    of leg F): a ledger connection that never answers must end at the
    helper's OWN cap with the DESIGNED assertion ("fewer than N jobs
    claimed"), never consume the scenario's 240s pytest-timeout budget
    on a single unbounded fetch. UNCURED the read hangs forever; CURED
    every fetch is bounded and the cap decides."""
    from tests.system_e2e.test_shutdown_requeue_states import _wait_running

    started = time.monotonic()
    with pytest.raises(AssertionError, match="fewer than 2 jobs claimed"):
        await asyncio.wait_for(
            _wait_running(_HangingConn(), "tq_pin_schema", _TAG, want=2, cap_secs=30.0),
            timeout=_PIN_WAIT_S,
        )
    assert time.monotonic() - started < _PIN_WAIT_S, (
        "the designed cap, not the outer budget, must decide"
    )


# ── pin 3: the cleanup completes through a fresh connection ────────────


@pytest.mark.timeout(180)
async def test_tagged_population_deleted_even_when_the_ledger_conn_died(
    module_pg_schema: ModulePgSchema,
) -> None:
    """The contamination root (legs E + F2): the module's cleanup runs
    on the ledger connection — when that socket dies mid-scenario, the
    UNBOUNDED cleanup hangs the test's finally (the zombie task) or
    dies with it, and the zombie population (same module schema, same
    tag) contaminates the SIBLING scenario's own population. CURED the
    cleanup is bounded and, on a dead ledger connection, retries ONCE
    on a FRESH connection: the rows the scenario owns are deleted, and
    the sibling's population stays clean."""
    import asyncpg

    from tests.system_e2e._invariants import delete_tagged_resilient

    schema = module_pg_schema.schema_name
    conn: asyncpg.Connection = await asyncpg.connect(module_pg_schema.pg_dsn)
    probe: asyncpg.Connection = await asyncpg.connect(module_pg_schema.pg_dsn)
    try:
        worker_id = new_uuid()
        seeded = [
            await create_running_job(conn, schema, worker_id, with_events=False) for _ in range(2)
        ]
        # The helper seeds the row's shape, not its tag; the tag is what
        # delete_tagged's population IS.
        for job_id in seeded:
            await conn.execute(
                f'UPDATE "{schema}".jobs SET tags = ARRAY[$2::text] WHERE id = $1::uuid',  # noqa: S608  # Why: schema is the fixture's validated identifier; the rest is $-bound.
                job_id,
                _TAG,
            )

        # The socket death: terminate the ledger connection's own
        # backend (the honest fast-error replay of the CI leg's dead
        # socket; the hang class is pin 2's replay).
        await probe.execute("SELECT pg_terminate_backend($1::int)", conn.get_server_pid())

        await delete_tagged_resilient(conn, module_pg_schema.pg_dsn, schema, _TAG)

        for job_id in seeded:

            async def _gone(job_id: UUID = job_id) -> bool:
                row = await probe.fetchrow(
                    f'SELECT 1 FROM "{schema}".jobs WHERE id = $1::uuid',  # noqa: S608  # Why: schema is the fixture's validated identifier; the id is $-bound.
                    job_id,
                )
                return row is None

            await wait_for_condition(_gone, description=f"job {job_id} deleted", timeout=30.0)
    finally:
        # The population law: even a red pin leaves no rows behind.
        await _delete_tagged_direct(probe, schema, _TAG)
        await conn.close()
        await probe.close()


async def _delete_tagged_direct(conn: asyncpg.Connection, schema: str, tag: str) -> None:
    """The pin's own teardown delete (a direct conn, no helper): the
    residue law — no pin's population outlives the pin."""
    import contextlib

    for table in ("jobs_archive", "jobs"):
        with contextlib.suppress(Exception):
            await conn.execute(
                f'DELETE FROM "{schema}".{table} WHERE tags @> ARRAY[$1::text]',  # noqa: S608  # Why: schema is the fixture's validated identifier; the tag is $-bound.
                tag,
            )

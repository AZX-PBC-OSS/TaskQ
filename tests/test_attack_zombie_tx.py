# ruff: noqa: S608  # Why: schema is a fixed test identifier, not user input; every value is $-bound.
"""The zombie transaction: the uncertain commit.

The partition lands DURING a terminal write's commit: Postgres processed
the COMMIT, the acknowledgement never reached the worker (the socket died
at the commit instant). The row is durably terminal -- with its attempt
row and its event -- while the worker's code paths classify the write as
FAILED. Every recovery path must then treat the durable row as the truth:

* the terminal write's own retry re-runs the fused statement against the
  row it already terminalised; the fencing WHERE (``status = 'running'``
  + worker + attempt + claim_epoch) matches nothing, so the re-run writes
  NOTHING (no second attempt row, no second event) and yields the fence
  outcome (``False`` / the fenced-out ``None``), which the handlers read
  as their no-op contract;
* the reclaim sweep's re-pend predicates filter ``status = 'running'``,
  so a terminal zombie row is never re-pended -- the work never re-runs;
* the enqueue's fresh-connection retry collides with the committed
  INSERT and must resolve to the SAME job id: with an idempotency key the
  (scope, key) arbiter dedupes the re-run; without one the id itself is
  the identity and the jobs-pkey collision is the existence proof, read
  back by id (``_read_back_landed_enqueue``);
* the transactional consumer's COMMIT is the same zombie point for the
  whole unit of work (terminal row + attempt + event + the actor's in-tx
  sub-enqueue): the tx lands, the worker sees the commit die, routes to
  the terminal handlers, whose write fences out -- and nothing in the
  recovery double-writes or strands the committed children.

The injection is a TCP proxy between the client pool and Postgres that
swallows a victim statement's response after forwarding the client's Sync
(asyncpg sends no Sync during prepare -- flush only -- so the one Sync of
a statement's execute is the point the statement, and a single-statement
autocommit write's COMMIT, is processed server-side, before any response
byte flies). The client hangs on the read while the commit is durable;
the test proves the commit on a separate admin connection, then kills the
client socket: the worker sees connection death, the database committed.
For the claim zombie the proxy can instead DELAY the ack (hold, let the
world move, then relay the held bytes), so the worker's own claim view
arrives after the reclaim and re-claim: its late-arriving terminal write
presents a stale epoch and must fence out.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import timedelta
from uuid import UUID

import asyncpg
import pytest
import structlog
from opentelemetry import trace
from pydantic import BaseModel

from taskq._ids import new_base62, new_uuid
from taskq.backend._dispatch import _dispatch_batch
from taskq.backend._enqueue import _enqueue, _enqueue_on_conn
from taskq.backend._protocol import (
    ConnLike,
    EnqueueArgs,
    ErrorInfo,
    IdempotencyKey,
    JobId,
    JobRow,
)
from taskq.backend._sql_templates import SqlTemplates, render
from taskq.backend._sweeps import sweep_expired_locks
from taskq.backend._terminal import _mark_failed_or_retry, _mark_succeeded
from taskq.backend.clock import SystemClock
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.context import JobContext
from taskq.retry import safe_mark_failed_or_retry
from taskq.testing.actor import EmptyPayload, default_actor_config
from taskq.testing.fixtures import _open_pg_backend
from taskq.testing.jobs import make_enqueue_args
from taskq.testing.pg import create_worker
from taskq.worker._consumer import _consume_transactional
from taskq.worker._handlers import _terminal_write_with_retry

pytestmark = [pytest.mark.integration, pytest.mark.slow]

_ARM_TIMEOUT_S = 10.0

# Distinct per-statement Parse-text markers. A Parse frame's payload is the
# full statement text; each marker appears in exactly one statement kind the
# victim connection can run (the mark_* templates set the other statuses, the
# claim template owns the epoch stamp, asyncpg's transaction spells 'COMMIT;').
_MARK_SUCCEEDED_MARKER = b"SET status = 'succeeded'"
_MARK_FAILED_MARKER = b"SET status = 'failed'"
_ENQUEUE_MARKER = b"INSERT INTO"
_CLAIM_MARKER = b"claim_epoch = j.claim_epoch + 1"
_COMMIT_MARKER = b"COMMIT;"


@dataclass
class _ConnState:
    """Per proxied connection wire state."""

    marker: bytes | None = None
    victim_stmt: bytes | None = None
    hold: bool = False
    held: list[bytes] = field(default_factory=list)


class _ZombieProxy:
    """TCP proxy that manufactures the uncertain commit.

    ``arm_zombie(marker)``: the next connection that parses a statement
    carrying *marker* gets every server-to-client byte swallowed after its
    execute Sync. The statement (and its autocommit) completes server-side;
    the client hangs awaiting the response; ``kill_zombie()`` then closes
    the client socket -- the worker sees connection death, the commit
    landed. ``arm_delayed(marker)``: same arming, but the held bytes are
    buffered and ``release_delayed_ack()`` relays them, so the original
    call completes LATE with its original view -- the delayed-ack zombie.
    Arming is consumed once: the retry's own Parse carries the same text
    and must relay untouched.

    The connection's first messages are UNTYPED (a 4-byte length then the
    body: SSLRequest, then the StartupMessage): the client-to-server pump
    forwards that phase raw, byte-exact, and starts frame parsing at the
    first typed message.
    """

    def __init__(self, host: str, port: int, database: str, user: str, password: str) -> None:
        self._target = (host, port)
        self._creds = (database, user, password)
        self._server: asyncio.Server | None = None
        self._armed_marker: bytes | None = None
        self._delay_mode = False
        self._victim_writer: asyncio.StreamWriter | None = None
        self._victim_state: _ConnState | None = None
        self._handlers: set[asyncio.Task[None]] = set()
        self.zombie_sync_forwarded = asyncio.Event()

    async def start(self) -> str:
        self._server = await asyncio.start_server(self._serve, "127.0.0.1", 0)
        port = self._server.sockets[0].getsockname()[1]
        database, user, password = self._creds
        return f"postgresql://{user}:{password}@127.0.0.1:{port}/{database}"

    async def stop(self) -> None:
        if self._server is not None:
            self._server.close()
            # The handler tasks end with their connections; cancelling the
            # stragglers keeps loop shutdown free of pending-task residue.
            for task in list(self._handlers):
                task.cancel()
            import contextlib

            with contextlib.suppress(asyncio.TimeoutError, Exception):
                await asyncio.wait_for(self._server.wait_closed(), timeout=2.0)
            self._server = None

    def arm_zombie(self, marker: bytes) -> None:
        self._armed_marker = marker
        self._delay_mode = False

    def arm_delayed(self, marker: bytes) -> None:
        self._armed_marker = marker
        self._delay_mode = True

    async def kill_zombie(self) -> None:
        """Close the victim client's socket: the ack is lost for good."""
        assert self._victim_writer is not None, "no zombie client to kill"
        self._victim_writer.close()
        self._victim_writer = None

    async def release_delayed_ack(self) -> None:
        """Relay the held response bytes: the original call completes late."""
        state = self._victim_state
        assert state is not None and state.hold, "no delayed ack to release"
        state.hold = False
        writer = self._victim_writer
        if writer is not None and not writer.is_closing():
            for chunk in state.held:
                writer.write(chunk)
            await writer.drain()
        state.held.clear()
        self._victim_writer = None

    async def wait_sync_forwarded(self) -> None:
        await asyncio.wait_for(self.zombie_sync_forwarded.wait(), timeout=_ARM_TIMEOUT_S)
        self.zombie_sync_forwarded.clear()

    async def _serve(
        self, client_reader: asyncio.StreamReader, client_writer: asyncio.StreamWriter
    ) -> None:
        import os

        server_reader, server_writer = await asyncio.open_connection(*self._target)
        state = _ConnState()
        debug = bool(os.environ.get("ZOMBIE_PROXY_DEBUG"))

        async def c2s() -> None:
            buf = b""
            startup_done = False
            while True:
                chunk = await client_reader.read(65536)
                if not chunk:
                    break
                buf += chunk
                while True:
                    if not startup_done:
                        if len(buf) < 8:
                            break
                        total = int.from_bytes(buf[0:4], "big")
                        code = int.from_bytes(buf[4:8], "big")
                        if code in (80877103, 80877102):  # SSLRequest / GSSENC
                            if len(buf) < total:
                                break
                            server_writer.write(buf[:total])
                            await server_writer.drain()
                            buf = buf[total:]
                            continue
                        # The real StartupMessage (or CancelRequest): forward
                        # it whole; the typed frame protocol follows it.
                        if len(buf) < total:
                            break
                        server_writer.write(buf[:total])
                        await server_writer.drain()
                        buf = buf[total:]
                        startup_done = True
                        continue
                    if len(buf) < 5:
                        break
                    mtype = buf[0:1]
                    length = int.from_bytes(buf[1:5], "big")
                    if len(buf) < 1 + length:
                        break
                    frame, payload = buf[: 1 + length], buf[5 : 1 + length]
                    buf = buf[1 + length :]
                    if debug and mtype == b"P":
                        _parts = payload.split(b"\x00", 1)
                        _qhead = _parts[1][:72] if len(_parts) > 1 else b""
                        print(f"[zproxy] parse head={_qhead!r}", flush=True)
                    if debug:
                        print(
                            f"[zproxy] c2s {mtype!r} len={length} "
                            f"victim_stmt={state.victim_stmt!r}",
                            flush=True,
                        )
                    if (
                        mtype == b"P"
                        and self._armed_marker is not None
                        and self._armed_marker in payload
                        and state.victim_stmt is None
                    ):
                        # The Parse payload is stmt-name\0query-text\0...: the
                        # victim's prepared statement NAME, so its own Bind
                        # can be identified below (asyncpg's first use of a
                        # statement whose param types the server must infer
                        # interleaves its own introspection round trips --
                        # their Parses, Binds and Syncs -- between the
                        # victim's prepare and the victim's execute).
                        state.victim_stmt = payload.split(b"\x00", 1)[0]
                        if debug:
                            print(
                                f"[zproxy] >>> victim Parse seen stmt={state.victim_stmt!r}",
                                flush=True,
                            )
                    if (
                        mtype == b"B"
                        and state.victim_stmt is not None
                        and not state.hold
                        and payload.split(b"\x00", 2)[1] == state.victim_stmt
                    ):
                        # The victim statement's OWN Bind: Bind/Execute/Sync
                        # ride one packet, so the statement -- and its
                        # autocommit COMMIT -- is processed server-side as
                        # soon as these bytes fly. Hold every response from
                        # here on.
                        state.hold = True
                        self._armed_marker = None  # consumed: the retry must relay untouched
                        self._victim_writer = client_writer
                        self._victim_state = state
                        self.zombie_sync_forwarded.set()
                        if debug:
                            print("[zproxy] >>> hold armed at victim Bind", flush=True)
                    if (
                        mtype == b"Q"
                        and self._armed_marker is not None
                        and not state.hold
                        and self._armed_marker in payload
                    ):
                        # The simple-query arm: asyncpg's argless execute()
                        # (the transaction boundary's COMMIT; among others)
                        # skips Parse/Bind and rides one 'Q' frame, whose
                        # processing -- and whose COMMIT -- is server-side
                        # before the CommandComplete flies.
                        state.hold = True
                        self._armed_marker = None  # consumed
                        self._victim_writer = client_writer
                        self._victim_state = state
                        self.zombie_sync_forwarded.set()
                        if debug:
                            print("[zproxy] >>> hold armed at simple query", flush=True)
                    server_writer.write(frame)
                    await server_writer.drain()

        async def s2c() -> None:
            while True:
                chunk = await server_reader.read(65536)
                if not chunk:
                    break
                if state.hold:
                    if debug:
                        print(f"[zproxy] s2c held {len(chunk)} bytes", flush=True)
                    if self._delay_mode:
                        state.held.append(chunk)
                    continue  # swallow: the commit is durable, the ack never flies
                if debug:
                    print(f"[zproxy] s2c relay {len(chunk)} bytes", flush=True)
                client_writer.write(chunk)
                await client_writer.drain()

        tasks = [asyncio.create_task(c2s()), asyncio.create_task(s2c())]
        handler: asyncio.Task[None] | None = asyncio.current_task()
        assert handler is not None
        self._handlers.add(handler)
        try:
            # Either direction dying ends the relay; the other is cancelled.
            await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for t in tasks:
                t.cancel()
            # Await the cancelled pumps so no pump task outlives the handler
            # (the loop-shutdown residue guard fails on any pending task).
            await asyncio.gather(*tasks, return_exceptions=True)
            server_writer.close()
            self._handlers.discard(handler)


# ── Shared helpers ──────────────────────────────────────────────────────


def _dsn_parts(dsn: str) -> tuple[str, int, str, str, str]:
    """Split ``postgresql://user:pass@host:port/db`` into its parts."""
    rest = dsn.removeprefix("postgresql://")
    creds, _, hostport_db = rest.partition("@")
    user, _, password = creds.partition(":")
    hostport, _, database = hostport_db.partition("/")
    host, _, port_s = hostport.partition(":")
    return host, int(port_s), database, user, password


async def _zombie_pool(proxy_dsn: str) -> asyncpg.Pool:
    return await asyncpg.create_pool(proxy_dsn, min_size=1, max_size=1)


async def _job_state(dsn: str, schema: str, job_id: JobId) -> asyncpg.Record | None:
    conn = await asyncpg.connect(dsn)
    try:
        return await conn.fetchrow(
            f'SELECT status, attempt, claim_epoch FROM "{schema}".jobs WHERE id = $1', job_id
        )
    finally:
        await conn.close()


async def _scalar(dsn: str, sql: str, job_id: JobId) -> int:
    conn = await asyncpg.connect(dsn)
    try:
        return await conn.fetchval(sql, job_id)
    finally:
        await conn.close()


async def _wait_status(
    dsn: str, schema: str, job_id: JobId, expected: str, timeout_s: float = 10.0
) -> asyncpg.Record:
    """Poll a separate admin connection until the row's committed status is
    *expected*: the injection's self-check. The Sync forward only proves the
    server STARTED the statement's final round; the durable row is the truth
    a timeout here refuses to fake."""
    deadline = asyncio.get_running_loop().time() + timeout_s
    while True:
        row = await _job_state(dsn, schema, job_id)
        if row is not None and row["status"] == expected:
            return row
        if asyncio.get_running_loop().time() > deadline:
            conn = await asyncpg.connect(dsn)
            try:
                all_rows = await conn.fetch(
                    f'SELECT id, status, actor FROM "{schema}".jobs LIMIT 10'
                )
            finally:
                await conn.close()
            pytest.fail(
                f"the zombie injection never landed the {expected!r} commit "
                f"(row reads {row['status'] if row else None!r}; table holds "
                f"{[dict(r) for r in all_rows]}): the injection "
                "self-check failed, a pass would prove nothing"
            )
        await asyncio.sleep(0.05)


def _succeeded_events_sql(schema: str) -> str:
    return (
        f'SELECT count(*) FROM "{schema}".job_events WHERE job_id = $1 '
        "AND detail->>'to_state' = 'succeeded'"
    )


class _JobStub:
    """The JobRow fields ``_terminal_write_with_retry``'s logging reads."""

    id: JobId
    actor: str = "test_actor"
    attempt: int = 1

    def __init__(self, job_id: JobId) -> None:
        self.id = job_id


class _PoolBackend:
    """Minimal Backend stand-in routing mark_failed_or_retry to the module
    function on the zombie pool (safe_mark_failed_or_retry needs a Backend
    only to call mark_failed_or_retry on it)."""

    def __init__(self, pool: asyncpg.Pool, sql: SqlTemplates) -> None:
        self._pool = pool
        self._sql = sql

    async def mark_failed_or_retry(self, **kwargs: object) -> JobRow | None:
        return await _mark_failed_or_retry(
            self._pool,
            self._sql,
            kwargs["job_id"],
            kwargs["worker_id"],
            kwargs["error_info"],
            kwargs["retry_delay"],
            attempt=kwargs.get("attempt"),
            claim_epoch=kwargs.get("claim_epoch"),
        )


_INFRA_DEATH: tuple[type[BaseException], ...] = (
    asyncpg.PostgresError,
    asyncpg.InterfaceError,
    OSError,
)


async def _claimed_running_job(
    pool: asyncpg.Pool, sql: SqlTemplates, schema: str, worker_id: UUID, args: EnqueueArgs
) -> JobRow:
    """Enqueue through the pool and claim the row with a real dispatch;
    returns the dispatched JobRow."""
    await _enqueue(pool, sql, schema, SystemClock(), args)
    claimed = await _dispatch_batch(
        pool, sql, 1, 1.0, schema, worker_id, ["default"], 1, timedelta(seconds=60)
    )
    assert len(claimed) == 1
    return claimed[0]


# ── Attack 1: the mark_succeeded zombie ────────────────────────────────


async def test_mark_succeeded_zombie_commit_lands_worker_sees_death(
    pg_dsn: str,
) -> None:
    """The commit landed; the worker saw the connection die; the retry must
    fence cleanly and write nothing, and the reclaim must never re-pend."""
    schema = f"tqr_{new_base62()}".lower()
    proxy = _ZombieProxy(*_dsn_parts(pg_dsn))
    proxy_dsn = await proxy.start()
    stack, deps, _backend = await _open_pg_backend(proxy_dsn, schema_name=schema)
    try:
        sql = render(schema)
        worker_id = new_uuid()
        async with deps.worker_pool.acquire() as conn:
            await create_worker(conn, schema, worker_id)
        args = make_enqueue_args()
        job = await _claimed_running_job(deps.worker_pool, sql, schema, worker_id, args)
        job_id = job.id

        proxy.arm_zombie(_MARK_SUCCEEDED_MARKER)
        pool = await _zombie_pool(proxy_dsn)

        async def _dying_write() -> bool:
            return await _mark_succeeded(
                pool,
                sql,
                job_id,
                worker_id,
                {"ok": True},
                attempt=1,
                claim_epoch=1,
            )

        # The zombie: the write does NOT return -- its response is swallowed
        # while the commit lands. The kill comes only after the durable row
        # is proven, so the worker's error is genuinely the lost ack of a
        # COMMITTED write.
        dying_task = asyncio.create_task(_dying_write())
        await proxy.wait_sync_forwarded()

        # Injection self-check: the commit IS durable server-side.
        await _wait_status(pg_dsn, schema, job_id, "succeeded")

        await proxy.kill_zombie()
        with pytest.raises(_INFRA_DEATH):
            await asyncio.wait_for(dying_task, timeout=_ARM_TIMEOUT_S)

        # The worker's recovery: the same write re-run through the consumer's
        # bounded retry. The fence matches nothing (the row is terminal), so
        # the re-run yields False -- the answer, not an outage -- and writes
        # nothing.
        landed = await _terminal_write_with_retry(
            lambda: _mark_succeeded(
                pool,
                sql,
                job_id,
                worker_id,
                {"ok": True},
                attempt=1,
                claim_epoch=1,
            ),
            log=structlog.get_logger("taskq.test.zombie"),
            job=_JobStub(job_id),  # type: ignore[arg-type]
            write_name="mark_succeeded",
        )
        assert landed is False

        attempts = await _scalar(
            pg_dsn,
            f'SELECT count(*) FROM "{schema}".job_attempts WHERE job_id = $1',
            job_id,
        )
        assert attempts == 1, f"the zombie retry double-wrote the attempt row ({attempts})"
        events = await _scalar(pg_dsn, _succeeded_events_sql(schema), job_id)
        assert events == 1, f"the zombie retry double-wrote the state_change event ({events})"

        # The reclaim's re-pend predicates filter status='running': the
        # terminal zombie row is never re-pended, the work never re-runs.
        reclaim_conn = await asyncpg.connect(pg_dsn)
        try:
            reclaimed = await sweep_expired_locks(
                reclaim_conn, timedelta(0), timedelta(0), schema=schema
            )
            assert reclaimed == 0
            row_after = await _job_state(pg_dsn, schema, job_id)
            assert row_after is not None and row_after["status"] == "succeeded"
        finally:
            await reclaim_conn.close()
    finally:
        await stack.aclose()
        await proxy.stop()


async def test_mark_failed_zombie_retry_surfaces_fenced_outcome(pg_dsn: str) -> None:
    """The failure write committed, the ack lost: the re-run's fence matches
    nothing and raises WorkerOwnershipMismatch, which the handler's
    safe wrapper reads as its fenced-out None; nothing double-writes."""
    schema = f"tqr_{new_base62()}".lower()
    proxy = _ZombieProxy(*_dsn_parts(pg_dsn))
    proxy_dsn = await proxy.start()
    stack, deps, _backend = await _open_pg_backend(proxy_dsn, schema_name=schema)
    try:
        sql = render(schema)
        worker_id = new_uuid()
        async with deps.worker_pool.acquire() as conn:
            await create_worker(conn, schema, worker_id)
        args = make_enqueue_args(retry_kind="non_retryable")
        job = await _claimed_running_job(deps.worker_pool, sql, schema, worker_id, args)
        job_id = job.id

        proxy.arm_zombie(_MARK_FAILED_MARKER)
        pool = await _zombie_pool(proxy_dsn)
        error_info = ErrorInfo(error_class="ValueError", error_message="boom", error_traceback=None)

        async def _dying_write() -> JobRow:
            return await _mark_failed_or_retry(
                pool,
                sql,
                job_id,
                worker_id,
                error_info,
                None,  # retry_delay=None: the terminal failed arm
                attempt=1,
                claim_epoch=1,
            )

        dying_task = asyncio.create_task(_dying_write())
        await proxy.wait_sync_forwarded()
        await _wait_status(pg_dsn, schema, job_id, "failed")
        await proxy.kill_zombie()
        with pytest.raises(_INFRA_DEATH):
            await asyncio.wait_for(dying_task, timeout=_ARM_TIMEOUT_S)

        landed_row = await safe_mark_failed_or_retry(
            _PoolBackend(pool, sql),  # type: ignore[arg-type]
            job_id,
            worker_id,
            error_info,
            None,
            attempt=1,
            claim_epoch=1,
        )
        assert landed_row is None, (
            "the zombie re-run must surface as the fenced-out None (the row "
            "is already terminal by the first attempt's commit), never as a "
            "second failure write"
        )
        attempts = await _scalar(
            pg_dsn,
            f'SELECT count(*) FROM "{schema}".job_attempts WHERE job_id = $1',
            job_id,
        )
        assert attempts == 1, f"the zombie re-run double-wrote the attempt row ({attempts})"
    finally:
        await stack.aclose()
        await proxy.stop()


# ── Attack 2: the enqueue zombie ───────────────────────────────────────


async def test_enqueue_zombie_no_key_retry_returns_same_job_id(pg_dsn: str) -> None:
    """The INSERT committed, the ack lost, no idempotency key.

    Two contracts. First, the zombie surfaces HONESTLY: the call raises the
    connection death, no silent duplicate, and the committed row -- the
    caller's own id -- stands exactly once. Second, the fresh-connection
    retry's own re-run (the wrapper's second arm, the shape its docstring
    names: a driver-internal state error at the write's acknowledgement,
    before mark_wrote) collides with the committed row on jobs_pkey and must
    read it back and return the SAME job id, never a raw violation for work
    that succeeded."""
    schema = f"tqr_{new_base62()}".lower()
    proxy = _ZombieProxy(*_dsn_parts(pg_dsn))
    proxy_dsn = await proxy.start()
    stack, _deps, _backend = await _open_pg_backend(proxy_dsn, schema_name=schema)
    try:
        sql = render(schema)
        args = make_enqueue_args()
        proxy.arm_zombie(_ENQUEUE_MARKER)
        pool = await _zombie_pool(proxy_dsn)

        async def _dying_enqueue() -> JobRow:
            return await _enqueue(pool, sql, schema, SystemClock(), args)

        dying_task = asyncio.create_task(_dying_enqueue())
        await proxy.wait_sync_forwarded()
        await _wait_status(pg_dsn, schema, args.id, "pending")
        await proxy.kill_zombie()
        with pytest.raises(_INFRA_DEATH):
            await asyncio.wait_for(dying_task, timeout=_ARM_TIMEOUT_S)

        # Contract 1: the zombie surfaced honestly, the row stands once.
        count = await _scalar(
            pg_dsn, f'SELECT count(*) FROM "{schema}".jobs WHERE id = $1', args.id
        )
        assert count == 1, f"the zombie enqueue left {count} rows for id {args.id}"

        # Contract 2: the wrapper's own re-run (attempt 2 of ONE _enqueue
        # call) hits the committed row and recovers it by id. Attempt 1 dies
        # at the write's acknowledgement with the driver-internal state
        # error (the parked-protocol shape _with_fresh_connection_retry's
        # docstring describes verbatim), unmarked (mark_wrote never ran:
        # the ack was lost), so the wrapper re-runs the op on a fresh
        # connection; the re-run's real INSERT collides with the really
        # committed row, and the read-back returns it.
        from unittest.mock import patch

        from asyncpg.exceptions import InternalClientError

        real_enqueue_on_conn = _enqueue_on_conn
        ack_died = {"first": True}

        async def _ack_zombie_then_real(conn: ConnLike, *rest: object, **kw: object) -> JobRow:
            if ack_died["first"]:
                ack_died["first"] = False
                raise InternalClientError(
                    "cannot switch to state 15; another operation (2) is in progress"
                )
            return await real_enqueue_on_conn(conn, *rest, **kw)

        with patch("taskq.backend._enqueue._enqueue_on_conn", _ack_zombie_then_real):
            recovered = await _enqueue(pool, sql, schema, SystemClock(), args)
        assert recovered.id == args.id, (
            f"CONTRACT: the wrapper's re-run over a zombie-committed row "
            f"returns the SAME job id; got {recovered.id} for {args.id}"
        )
        count = await _scalar(
            pg_dsn, f'SELECT count(*) FROM "{schema}".jobs WHERE id = $1', args.id
        )
        assert count == 1, f"the re-run duplicated the committed row ({count})"
    finally:
        await stack.aclose()
        await proxy.stop()


async def test_enqueue_zombie_with_key_retry_returns_same_job_id(pg_dsn: str) -> None:
    """Same zombie shape with an idempotency key: the (scope, key) arbiter
    dedupes the re-run to the committed row -- the same job id."""
    schema = f"tqr_{new_base62()}".lower()
    proxy = _ZombieProxy(*_dsn_parts(pg_dsn))
    proxy_dsn = await proxy.start()
    stack, _deps, _backend = await _open_pg_backend(proxy_dsn, schema_name=schema)
    try:
        sql = render(schema)
        key = IdempotencyKey(f"zombie-{new_base62()}")
        args = make_enqueue_args(idempotency_key=str(key))
        # The keyed enqueue's INSERT runs inside the bounded-idempotency
        # wait's transaction (a savepoint scope on an owned conn), so the
        # row commits only at the scope's COMMIT -- arm the zombie there,
        # never at the INSERT's Bind (whose held response would strand the
        # commit itself behind a hung client).
        proxy.arm_zombie(_COMMIT_MARKER)
        pool = await _zombie_pool(proxy_dsn)

        async def _dying_enqueue() -> JobRow:
            return await _enqueue(pool, sql, schema, SystemClock(), args)

        dying_task = asyncio.create_task(_dying_enqueue())
        await proxy.wait_sync_forwarded()
        await _wait_status(pg_dsn, schema, args.id, "pending")
        await proxy.kill_zombie()
        with pytest.raises(_INFRA_DEATH):
            await asyncio.wait_for(dying_task, timeout=_ARM_TIMEOUT_S)

        retried = await _enqueue(pool, sql, schema, SystemClock(), args)
        assert retried.id == args.id
        count = await _scalar(
            pg_dsn, f'SELECT count(*) FROM "{schema}".jobs WHERE id = $1', args.id
        )
        assert count == 1
    finally:
        await stack.aclose()
        await proxy.stop()


# ── Attack 3: the claim zombie (delayed ack, late-arriving write) ──────


async def test_claim_zombie_late_write_fenced_by_restamped_epoch(pg_dsn: str) -> None:
    """The claim committed, its ack delayed: the lease lapses, the reclaim
    re-pends, a new claimant re-stamps the epoch; the zombie worker's own
    (delayed) claim view then arrives and its late-arriving terminal write
    must fence out without touching the live attempt."""
    schema = f"tqr_{new_base62()}".lower()
    proxy = _ZombieProxy(*_dsn_parts(pg_dsn))
    proxy_dsn = await proxy.start()
    stack, deps, backend = await _open_pg_backend(proxy_dsn, schema_name=schema)
    try:
        sql = render(schema)
        zombie_worker = new_uuid()
        live_worker = new_uuid()
        async with deps.worker_pool.acquire() as conn:
            await create_worker(conn, schema, zombie_worker)
            await create_worker(conn, schema, live_worker)
        args = make_enqueue_args(max_attempts=3)
        await _enqueue(deps.worker_pool, sql, schema, SystemClock(), args)

        # The zombie claim: commits, its ack is DELAYED (the worker does not
        # learn of the claim until we release it).
        proxy.arm_delayed(_CLAIM_MARKER)
        zombie_pool = await _zombie_pool(proxy_dsn)
        claim_task = asyncio.create_task(
            _dispatch_batch(
                zombie_pool,
                sql,
                1,
                1.0,
                schema,
                zombie_worker,
                ["default"],
                1,
                timedelta(seconds=60),
            )
        )
        await proxy.wait_sync_forwarded()

        # The claim IS durable (injection self-check).
        row = await _job_state(pg_dsn, schema, args.id)
        assert row is not None and row["status"] == "running", (
            "the zombie claim must commit before the ack is held"
        )
        zombie_claim_epoch = row["claim_epoch"]
        zombie_attempt = row["attempt"]

        # The worker never knew: the lease lapses, the reclaim re-pends.
        admin = await asyncpg.connect(pg_dsn)
        try:
            await admin.execute(
                f'UPDATE "{schema}".jobs SET lock_expires_at = statement_timestamp() '
                "- interval '1 second' WHERE id = $1",
                args.id,
            )
            reclaimed = await sweep_expired_locks(admin, timedelta(0), timedelta(0), schema=schema)
            assert reclaimed == 1
            row = await _job_state(pg_dsn, schema, args.id)
            assert row is not None and row["status"] == "pending"
            # Make the re-pended row claimable NOW (the re-pend's own retry
            # curve may schedule it into the future; the curve is not this
            # pin's subject).
            await admin.execute(
                f'UPDATE "{schema}".jobs SET scheduled_at = statement_timestamp() '
                "- interval '1 second' WHERE id = $1",
                args.id,
            )
        finally:
            await admin.close()

        # A live claimant re-claims: the epoch is re-stamped.
        live_rows = await backend.dispatch_batch(
            live_worker, ["default"], limit=1, lock_lease=timedelta(seconds=60)
        )
        assert len(live_rows) == 1 and live_rows[0].id == args.id
        live_claim_epoch = live_rows[0].claim_epoch
        live_attempt = live_rows[0].attempt
        assert live_claim_epoch > zombie_claim_epoch

        # NOW the delayed ack reaches the zombie worker: its claim view
        # (attempt, claim_epoch as claimed) arrives, and it proceeds to its
        # terminal write LATE -- after the reclaim and re-claim.
        await proxy.release_delayed_ack()
        claimed_rows = await asyncio.wait_for(claim_task, timeout=_ARM_TIMEOUT_S)
        assert len(claimed_rows) == 1 and claimed_rows[0].id == args.id
        assert claimed_rows[0].claim_epoch == zombie_claim_epoch
        assert claimed_rows[0].attempt == zombie_attempt

        landed = await _mark_succeeded(
            zombie_pool,
            sql,
            args.id,
            zombie_worker,
            {"zombie": True},
            attempt=claimed_rows[0].attempt,
            claim_epoch=claimed_rows[0].claim_epoch,
        )
        assert landed is False, (
            "the zombie's late-arriving write must fence out: the claim epoch "
            "was re-stamped by the live claimant, the stale view matches nothing"
        )
        row = await _job_state(pg_dsn, schema, args.id)
        assert row is not None and row["status"] == "running"
        assert row["claim_epoch"] == live_claim_epoch
        attempts = await _scalar(
            pg_dsn,
            f'SELECT count(*) FROM "{schema}".job_attempts WHERE job_id = $1',
            args.id,
        )
        # Exactly ONE attempt row stands, and it is the RECLAIM SWEEP's own
        # crashed-attempt record (the sweep writes one per reclaim) -- never
        # a 'succeeded' row from the zombie's late-arriving write.
        assert attempts == 1, f"expected only the sweep's crashed-attempt row, found {attempts}"
        outcomes = await _scalar(
            pg_dsn,
            f'SELECT count(*) FROM "{schema}".job_attempts '
            "WHERE job_id = $1 AND outcome = 'succeeded'",
            args.id,
        )
        assert outcomes == 0, f"the fenced-out late write recorded a succeeded attempt ({outcomes})"
        assert live_attempt > zombie_attempt

        # The epoch conjunct IN ISOLATION (the attempt-saturation corner the
        # claim epoch exists for): a write from the CURRENT holder
        # presenting the row's CURRENT attempt with the STALE epoch -- the
        # shape a claim-clamped attempt number repeats at the smallint
        # ceiling (same worker, same attempt) while the epoch always
        # advances -- must fence out on the epoch alone.
        ceiling_shaped = await _mark_succeeded(
            zombie_pool,
            sql,
            args.id,
            live_worker,
            {"zombie": True},
            attempt=live_attempt,
            claim_epoch=zombie_claim_epoch,
        )
        assert ceiling_shaped is False, (
            "a write whose attempt matches but whose claim epoch is stale "
            "must fence out on the epoch conjunct alone"
        )
        row = await _job_state(pg_dsn, schema, args.id)
        assert row is not None and row["status"] == "running"
    finally:
        await stack.aclose()
        await proxy.stop()


# ── Attack 4: the batch/tx zombie (the multi-write transaction) ────────


async def test_tx_commit_zombie_whole_tx_lands_nothing_double_writes(
    pg_dsn: str,
) -> None:
    """The transactional consumer's COMMIT is the zombie point: the whole
    unit of work (terminal row + attempt + event + the actor's in-tx
    sub-enqueue) commits server-side; the worker sees the commit die. The
    recovery must leave the durable truth intact -- one attempt row, one
    event, the child row exactly once -- and never re-pend the row."""
    schema = f"tqr_{new_base62()}".lower()
    proxy = _ZombieProxy(*_dsn_parts(pg_dsn))
    proxy_dsn = await proxy.start()
    stack, deps, backend = await _open_pg_backend(proxy_dsn, schema_name=schema)
    try:
        sql = render(schema)
        worker_id = new_uuid()
        job_args = make_enqueue_args()
        job = await _claimed_running_job(deps.worker_pool, sql, schema, worker_id, job_args)

        transaction_conn = await asyncpg.connect(proxy_dsn)
        enqueuer = SubJobEnqueuer(
            loop_scope_resolved=None,
            worker_pool=deps.worker_pool,
            backend=backend,
            transaction_conn=transaction_conn,
        )
        child_args = make_enqueue_args(actor="child_actor")

        async def actor(job: JobRow, ctx: JobContext[BaseModel]) -> object:
            # The in-tx sub-enqueue joins the transaction on the real
            # backend (no simulation buffer): the child row commits with
            # the parent.
            await enqueuer._do_enqueue(child_args, None)
            return {"ok": True}

        ctx = JobContext(
            job_id=job.id,
            actor=job.actor,
            queue=job.queue,
            attempt=job.attempt,
            claim_epoch=job.claim_epoch,
            worker_id=worker_id,
            payload=EmptyPayload(),
            jobs=enqueuer,
            log=structlog.get_logger("taskq.test.zombie"),
        )

        proxy.arm_zombie(_COMMIT_MARKER)
        consume_task = asyncio.create_task(
            _consume_transactional(
                backend,
                job,
                worker_id,
                ctx,
                enqueuer,
                transaction_conn,
                actor,
                default_actor_config(),
                None,
                timedelta(hours=24),
                None,
                trace.get_current_span(),
                structlog.get_logger("taskq.test.zombie"),
            )
        )
        await proxy.wait_sync_forwarded()

        # The whole tx IS durable (injection self-check).
        row = await _wait_status(pg_dsn, schema, job.id, "succeeded")
        await proxy.kill_zombie()

        # The worker's recovery: the commit error routes to the terminal
        # handlers, whose write fences out against the committed row. The
        # outcome is conservative (the durable truth is succeeded), and the
        # recovery must not corrupt it.
        outcome = await asyncio.wait_for(consume_task, timeout=_ARM_TIMEOUT_S)
        assert outcome == "noop", (
            f"the commit-zombie recovery must read the fenced-out no-op, got {outcome}"
        )

        attempts = await _scalar(
            pg_dsn,
            f'SELECT count(*) FROM "{schema}".job_attempts WHERE job_id = $1',
            job.id,
        )
        assert attempts == 1, f"the tx zombie recovery double-wrote the attempt row ({attempts})"
        events = await _scalar(pg_dsn, _succeeded_events_sql(schema), job.id)
        assert events == 1, f"the tx zombie recovery double-wrote the event ({events})"
        child_count = await _scalar(
            pg_dsn, f'SELECT count(*) FROM "{schema}".jobs WHERE id = $1', child_args.id
        )
        assert child_count == 1, (
            f"the committed child row must survive exactly once ({child_count})"
        )
        row = await _job_state(pg_dsn, schema, job.id)
        assert row is not None and row["status"] == "succeeded"

        # The reclaim never re-pends the terminal tx-zombie row.
        reclaim_conn = await asyncpg.connect(pg_dsn)
        try:
            await sweep_expired_locks(reclaim_conn, timedelta(0), timedelta(0), schema=schema)
            row = await _job_state(pg_dsn, schema, job.id)
            assert row is not None and row["status"] == "succeeded"
        finally:
            await reclaim_conn.close()
    finally:
        await stack.aclose()
        await proxy.stop()

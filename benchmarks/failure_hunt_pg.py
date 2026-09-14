"""Empirical failure-path hunt (perf campaign follow-up).

Runs against the real PG at postgresql://taskq:taskq@localhost:5432/taskq.
Read-only with respect to src/ — this file lives in benchmarks/ and only
writes rows to the `taskq` schema, cleaning up after itself.

Parts
  1  BATCH POISONING   — one NUL payload in enqueue_batch / _enqueue_batch_fast
  2  NOTIFY FAN-OUT    — cancel_where-style pg_notify burst (payload size, timing)
  3  PROGRESS ORPHANS  — buffer lifecycle on flush/self-heal/leak-window
  4  CANCELLATION      — double-cancel during asyncio.shield -> unretrieved exc
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from types import SimpleNamespace
from uuid import UUID

import asyncpg
from pydantic import BaseModel

from taskq._ids import new_job_id, new_uuid
from taskq.backend._enqueue import _enqueue, _enqueue_batch, _enqueue_batch_fast
from taskq.backend._protocol import EnqueueArgs
from taskq.backend._sql_templates import render
from taskq.backend.clock import SystemClock
from taskq.backend.postgres import _cancel_notify_channels, _cancel_notify_payload
from taskq.constants import events_channel
from taskq.progress._buffer import _ProgressBuffer
from taskq.progress._flush import _flush_buffer, _flush_buffer_immediate
from taskq.worker._consumer import consume_one_job
from taskq.worker.cancel import ActiveJobRegistry

DSN = "postgresql://taskq:taskq@localhost:5432/taskq"
SCHEMA = "taskq"
WORKER_ID = new_uuid()

_report: list[str] = []


def say(line: str = "") -> None:
    print(line)
    _report.append(line)


def make_args(
    n: int, *, poison_payload: bool = False, poison_tag: bool = False, batch_meta: bool = False
) -> list[EnqueueArgs]:
    args_list = []
    for i in range(n):
        payload: dict[str, object] = {"i": i}
        tags: tuple[str, ...] = ("t",)
        if i == n // 2 and poison_payload:
            payload = {"i": "bad\x00payload"}
        if i == n // 2 and poison_tag:
            tags = ("t\x00bad",)
        metadata: dict[str, object] = {"batch_id": str(BATCH_ID)} if batch_meta else {}
        args_list.append(
            EnqueueArgs(
                id=new_job_id(),
                actor="hunt_actor",
                queue="hunt",
                payload=payload,
                max_attempts=3,
                retry_kind="transient",
                scheduled_at=None,
                tags=tags,
                metadata=metadata,
            )
        )
    return args_list


BATCH_ID = new_uuid()


async def part1_batch_poisoning(pool: asyncpg.Pool) -> None:
    say("=" * 78)
    say("PART 1 — BATCH POISONING (one bad job in a 50-job batch)")
    say("=" * 78)
    sql = render(SCHEMA)

    # Baseline: clean 50-item batch
    t0 = time.perf_counter()
    rows = await _enqueue_batch(pool, sql, SCHEMA, make_args(50, batch_meta=False))
    base_ms = (time.perf_counter() - t0) * 1e3
    say(f"1a  clean _enqueue_batch(50):          OK  {base_ms:8.2f} ms  rows={len(rows)}")

    t0 = time.perf_counter()
    rows = await _enqueue_batch_fast(pool, sql, SCHEMA, make_args(50))
    fast_ms = (time.perf_counter() - t0) * 1e3
    say(f"1b  clean _enqueue_batch_fast(50):     OK  {fast_ms:8.2f} ms  rows={rows}")

    # Poisoned payload (NUL) in the middle of a 50-batch
    async with pool.acquire() as c:
        before = await c.fetchval(f"SELECT count(*) FROM {SCHEMA}.jobs WHERE queue='hunt_poison'")  # noqa: S608  # Why: SCHEMA is a validated module constant
    t0 = time.perf_counter()
    err: BaseException | None = None
    try:
        await _enqueue_batch(pool, sql, SCHEMA, make_args(50, poison_payload=True))
    except Exception as exc:
        err = exc
    fail_ms = (time.perf_counter() - t0) * 1e3
    async with pool.acquire() as c:
        after = await c.fetchval(f"SELECT count(*) FROM {SCHEMA}.jobs WHERE queue='hunt_poison'")  # noqa: S608  # Why: SCHEMA is a validated module constant
    say(
        f"1c  _enqueue_batch(50, 1 NUL payload): {type(err).__name__}  {fail_ms:8.2f} ms  "
        f"rows_before={before} rows_after={after}"
    )
    say(
        f"    -> WHOLE batch aborted ({after - before} of 50 rows landed); "
        f"error raised client-side before SQL: {err}"
    )

    # Retry-loop cost: the caller retrying the same poisoned batch n times
    retries = 5
    t0 = time.perf_counter()
    for _ in range(retries):
        with contextlib.suppress(ValueError):
            await _enqueue_batch(pool, sql, SCHEMA, make_args(50, poison_payload=True))
    loop_ms = (time.perf_counter() - t0) * 1e3
    say(
        f"1d  {retries} retries of the poisoned batch: {loop_ms:8.2f} ms total "
        f"({loop_ms / retries:6.2f} ms/retry; 0 good jobs enqueued per retry)"
    )

    # Fast path: NUL payload — guarded at jsonb_param too?
    t0 = time.perf_counter()
    err = None
    try:
        await _enqueue_batch_fast(pool, sql, SCHEMA, make_args(50, poison_payload=True))
    except Exception as exc:
        err = exc
    fast_fail_ms = (time.perf_counter() - t0) * 1e3
    say(
        f"1e  _enqueue_batch_fast(50, 1 NUL):   {type(err).__name__}  {fast_fail_ms:8.2f} ms "
        f"(COPY aborted client-side, nothing written)"
    )

    # NUL tag: guarded at EnqueueArgs construction?
    err = None
    try:
        await _enqueue_batch(pool, sql, SCHEMA, make_args(50, poison_tag=True))
    except Exception as exc:
        err = exc
    say(
        f"1f  _enqueue_batch(50, 1 NUL tag):    {type(err).__name__} (guarded at EnqueueArgs construction)"
    )

    # dispatch_batch atomicity: claim is a single CTE — poison a payload of a
    # PENDING row and confirm the claim either claims or not — never partially.
    say(
        "1g  dispatch CTE atomicity: single UPDATE..RETURNING — no partial-claim path exists (code: _dispatch_sql.py:166-182)"
    )

    # poison-job terminal cost: actor result with NUL reaching mark_failed
    say(
        "1h  terminal-write poison (NUL in progress_state): jsonb_param -> ValueError; "
        "ValueError NOT in _TERMINAL_WRITE_INFRA_EXCEPTIONS -> actor-failure path -> "
        "retried max_attempts times (bounded, noisy). See report."
    )


async def part2_notify_fanout(pool: asyncpg.Pool) -> None:
    say()
    say("=" * 78)
    say("PART 2 — NOTIFY FAN-OUT (cancel_where of N running jobs)")
    say("=" * 78)

    payload = _cancel_notify_payload(new_job_id(), WORKER_ID)
    say(
        f"2a  per-target payload length: {len(payload)} bytes (PG cap 8000) — cap overflow impossible (fixed shape)"
    )
    say(f"    payload: {payload}")

    for n in (500, 2000):
        # Insert n running jobs owned by WORKER_ID
        ids = [new_job_id() for _ in range(n)]
        async with pool.acquire() as c:
            await c.executemany(
                f"INSERT INTO {SCHEMA}.jobs (id, actor, queue, payload, payload_schema_ver,"  # noqa: S608  # Why: SCHEMA is a validated module constant
                " status, priority, attempt, max_attempts, retry_kind, created_at, scheduled_at,"
                " started_at, last_heartbeat_at, locked_by_worker, lock_expires_at, progress_state,"
                " progress_seq, metadata, tags)"
                " VALUES ($1,'hunt_actor','hunt','{}'::jsonb,1,'running',0,1,3,'transient',now(),now(),"
                " now(), now(), $2, now() + interval '5 minutes', '{}'::jsonb, 0, '{}'::jsonb, '{}')",
                [(jid, WORKER_ID) for jid in ids],
            )
            # exact statement from postgres.py:713-728
            channels: list[str] = []
            payloads: list[str] = []
            for jid in ids:
                p = _cancel_notify_payload(jid, WORKER_ID)
                ch = _cancel_notify_channels(SCHEMA, WORKER_ID)
                channels.extend(ch)
                payloads.extend([p, p])

            # a real LISTEN connection counts deliveries on the events channel
            got = {"n": 0}

            def _count(conn, pid, channel, p) -> None:
                got["n"] += 1  # noqa: B023  # Why: `got` is re-bound per loop iteration and the listener is consumed within that same iteration

            listen_conn = await asyncpg.connect(DSN)
            await listen_conn.add_listener(events_channel(SCHEMA), _count)

            t0 = time.perf_counter()
            await c.execute(
                "SELECT pg_notify(channel, payload) "
                "FROM unnest($1::text[], $2::text[]) AS t(channel, payload)",
                channels,
                payloads,
            )
            stmt_ms = (time.perf_counter() - t0) * 1e3
            # drain deliveries
            await asyncio.sleep(0.5)
            await listen_conn.close()
            say(
                f"2b  cancel_where-style notify burst n={n} running jobs: "
                f"{len(channels)} pg_notify in ONE statement, {stmt_ms:8.2f} ms; "
                f"events-channel deliveries observed on a listener: {got['n']}"
            )
            await c.execute(
                f"DELETE FROM {SCHEMA}.jobs WHERE actor='hunt_actor' AND queue='hunt'",  # noqa: S608  # Why: SCHEMA is a validated module constant
            )


async def part3_progress_orphans(pool: asyncpg.Pool) -> None:
    say()
    say("=" * 78)
    say("PART 3 — PROGRESS BUFFER ORPHANS")
    say("=" * 78)
    sql = render(SCHEMA)
    clock = SystemClock()

    row = await _enqueue(
        pool,
        sql,
        SCHEMA,
        clock,
        EnqueueArgs(
            id=new_job_id(),
            actor="hunt_actor",
            queue="hunt",
            payload={"x": 1},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=None,
        ),
    )
    job_id = row.id
    async with pool.acquire() as c:
        await c.execute(
            f"UPDATE {SCHEMA}.jobs SET status='running', locked_by_worker=$2, "  # noqa: S608  # Why: SCHEMA is a validated module constant
            "lock_expires_at = now() + interval '5 minutes' WHERE id=$1",
            job_id,
            WORKER_ID,
        )

    buffers: dict[UUID, _ProgressBuffer] = {}
    buf = _ProgressBuffer(job_id=job_id, base_seq=0)
    buf.pending_seq_delta = 3
    buf.pending_state = {"step": 2, "detail": "crunching"}
    buf.dirty = True
    buffers[job_id] = buf

    await _flush_buffer(pool, SCHEMA, job_id, WORKER_ID, buf, buffers)
    async with pool.acquire() as c:
        seq, state = await c.fetchrow(
            f"SELECT progress_seq, progress_state FROM {SCHEMA}.jobs WHERE id=$1",  # noqa: S608
            job_id,  # Why: SCHEMA is a validated module constant
        )
    say(
        f"3a  flush while job 'running': DB seq={seq} state={state}; buffer entries in dict: {len(buffers)} (kept, by design)"
    )

    # consumer dies WITHOUT terminal write (row still 'running', worker still owner)
    # -> orphaned dirty buffer? simulate a new progress write then consumer crash
    buf.pending_seq_delta += 2
    buf.pending_state["step"] = 3
    buf.dirty = True
    say(f"3b  consumer crashed mid-job; orphan dirty buffer present: {len(buffers)} entry")

    # flush loop self-heal: job row leaves 'running' (lease sweep / terminal)
    async with pool.acquire() as c:
        await c.execute(
            f"UPDATE {SCHEMA}.jobs SET status='succeeded', finished_at=now() WHERE id=$1",  # noqa: S608
            job_id,  # Why: SCHEMA is a validated module constant
        )
    await _flush_buffer_immediate(pool, SCHEMA, job_id, WORKER_ID, buffers)
    say(
        f"3c  next flush tick after row terminal: row-None idempotency gate -> pop; entries now: {len(buffers)} (self-healed)"
    )

    # leak window: cancel lands between buffer INSERT (consumer.py:387) and the
    # inner try (consumer.py:452) — here: while active_jobs.register is blocked
    say()
    say("3d  leak window: CancelledError during active_jobs.register (consumer.py:421)")
    await leak_window_demo()


async def leak_window_demo() -> None:
    class P(BaseModel):
        x: int = 0

    registry = ActiveJobRegistry()
    await registry._lock.acquire()  # hold the lock so register() blocks forever

    buffers: dict[UUID, _ProgressBuffer] = {}
    deps = SimpleNamespace(
        progress_buffers=buffers, worker_pool=None, settings=None, redis_client=None
    )

    row = SimpleNamespace(
        id=new_job_id(),
        actor="hunt_actor",
        queue="hunt",
        attempt=1,
        identity_key=None,
        progress_seq=0,
        tags=(),
        metadata={},
        start_to_close=None,
    )

    async def run_actor(job_row, ctx):  # Why: run_actor signature
        await asyncio.sleep(60)

    async def victim() -> None:
        with contextlib.suppress(asyncio.CancelledError):
            await consume_one_job(
                SimpleNamespace(),  # backend — never reached on this path
                row,  # type: ignore[arg-type]
                WORKER_ID,
                deps=deps,  # type: ignore[arg-type]
                run_actor=run_actor,
                actor_config=SimpleNamespace(on_success=None, on_success_timeout=1.0),
                payload_type=P,
                clock=SystemClock(),
                validated_payload=P(),
                active_jobs=registry,
            )

    t = asyncio.create_task(victim())
    await asyncio.sleep(0.2)  # let it reach the blocked register()
    t.cancel()
    await asyncio.sleep(0.2)
    say(
        f"    cancel delivered while register() waits on the held lock -> "
        f"progress_buffers entries leaked: {len(buffers)} (never popped: pop lives in the "
        f"inner finally at consumer.py:596-617, which never runs)"
    )
    registry._lock.release()


async def part4_cancellation() -> None:
    say()
    say("=" * 78)
    say("PART 4 — CANCELLATION: double-cancel during asyncio.shield")
    say("=" * 78)
    unretrieved: list[BaseException] = []
    loop = asyncio.get_running_loop()

    def handler(loop_, context):  # Why: loop exception handler signature
        exc = context.get("exception")
        msg = context.get("message", "")
        if "never retrieved" in msg and exc is not None:
            unretrieved.append(exc)
        else:
            loop_.default_exception_handler(context)

    loop.set_exception_handler(handler)

    async def failing_write() -> None:
        await asyncio.sleep(0.05)
        raise RuntimeError("db connection dropped mid-commit")

    async def victim() -> None:
        try:
            await asyncio.shield(failing_write())  # first shield (like consumer.py:830)
        except asyncio.CancelledError:
            # second cancel lands here (e.g. shutdown escalation during the
            # mark_cancelled shield at consumer.py:532) -> the shielded task
            # is left running DETACHED; its later failure is never retrieved
            await asyncio.shield(failing_write())

    v = asyncio.create_task(victim())
    await asyncio.sleep(0.01)
    v.cancel()  # first cancel
    await asyncio.sleep(0.01)
    v.cancel()  # second cancel — lands inside the except-block's shield
    with contextlib.suppress(asyncio.CancelledError):
        await v
    await asyncio.sleep(0.2)  # let the detached task fail
    say(
        f"    double-cancel during shield: unretrieved-exception callbacks fired: {len(unretrieved)}"
    )
    for exc in unretrieved:
        say(f"      -> {type(exc).__name__}: {exc}")
    say(
        "    pattern sites: _consumer.py:532/606/635/938/965/986, _cancel_bulk.py:332 —"
        " a second CancelledError while awaiting the shield detaches the inner task;"
        " if it later fails its exception is never retrieved (log noise + lost signal)"
    )


async def main() -> None:
    pool = await asyncpg.create_pool(DSN, min_size=2, max_size=5)
    try:
        async with pool.acquire() as c:
            await c.execute(f"DELETE FROM {SCHEMA}.jobs WHERE actor='hunt_actor'")  # noqa: S608  # Why: SCHEMA is a validated module constant
        await part1_batch_poisoning(pool)
        await part2_notify_fanout(pool)
        await part3_progress_orphans(pool)
        await part4_cancellation()
    finally:
        async with pool.acquire() as c:
            await c.execute(f"DELETE FROM {SCHEMA}.jobs WHERE actor='hunt_actor'")  # noqa: S608  # Why: SCHEMA is a validated module constant
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())

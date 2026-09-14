# Why: schema is a fixed test identifier, not user input; every value is $-bound.
"""Red-team locks: an open actor-body transaction's hold on cross-actor enqueue serialization.

The transactional consumer runs the actor inside its transaction
connection's transaction (``worker/_consumer.py`` — ``async with
transaction_conn.transaction():`` + ``SAVEPOINT _tq_actor`` around
``asyncio.wait_for(run_actor(...), timeout=timeout)``) and
``default_start_to_close`` is ``None`` by default (``settings.py`` —
"None (the default) means unbounded"), so the actor's database
transaction is open for the actor's whole runtime. Sub-enqueues made by
the actor (``ctx.jobs``) join that transaction, and the enqueue path
deliberately binds its serialization to a caller-owned transaction
(``backend/_enqueue.py`` — "A caller who already holds a transaction
owns the scope — the advisory locks then span that caller's
transaction"): the ``max_pending`` advisory lock and the idempotency
INSERT's ``ON CONFLICT (idempotency_scope, idempotency_key) ... DO
NOTHING`` speculative token (``backend/_sql_templates.py``).

Victim effects pinned here, from a second client connection while the
holder's transaction stays open and uncommitted:

* max_pending: the advisory-lock budget (5 s) bounds the other
  producer's wait and raises the typed ``MaxPendingLockTimeoutError``
  — pinned as today's designed observable; the defect it exposes is
  that the error storm lasts for the actor's whole runtime.
* idempotency: the same-key INSERT blocks on the uncommitted
  speculative token with NO bound at all — not the advisory budget, not
  ``lock_timeout``, nothing — until the holder's transaction resolves.
  The RED contract: an unbounded actor body must not hold cross-actor
  enqueue serialization; the desired behavior needs either a documented
  bound or decoupling.
"""

import asyncio
import contextlib
import dataclasses
import time

import asyncpg
import pytest

from taskq._ids import new_base62
from taskq.backend._enqueue import _enqueue_on_conn
from taskq.backend._sql_templates import render as render_sql
from taskq.backend.clock import SystemClock
from taskq.exceptions import IdempotencyKeyLockTimeoutError, MaxPendingLockTimeoutError
from taskq.migrate import apply_pending
from taskq.testing.jobs import make_enqueue_args

pytestmark = pytest.mark.integration

#: Window/margin geometry for the idempotency-token twin: the window sits
#: beyond the enqueue path's idempotency lock_timeout budget
#: (DEFAULT_IDEMPOTENCY_LOCK_TIMEOUT_MS = 5 s, backend/_enqueue.py) so the
#: designed typed refusal at the budget lands INSIDE the window; the margin
#: separates "the test's own wait_for ended the wait" (elapsed at the full
#: window — the unbounded-block regression) from any earlier exit (a bound
#: fired). Same geometry as the sweep/notify pool twin
#: (tests/test_rt_locks_sweep_notify_pool_unbounded.py).
_TEST_BOUND_S: float = 8.0
_UNBOUNDED_MARGIN_S: float = 7.5


async def _fresh_schema(pg_dsn: str) -> tuple[asyncpg.Connection, str]:
    """A caller-owned connection plus a freshly-migrated random schema."""
    schema = f"tlck_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await apply_pending(conn, schema=schema)
    except BaseException:
        with contextlib.suppress(Exception):
            await conn.close()
        raise
    return conn, schema


async def _drop_schema_quietly(pg_dsn: str, schema: str) -> None:
    """DROP the random schema on a fresh connection, robust to wedged conns."""
    with contextlib.suppress(Exception):
        conn = await asyncpg.connect(pg_dsn)
        try:
            await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        finally:
            with contextlib.suppress(Exception):
                await conn.close()


async def test_open_tx_max_pending_lock_times_out_other_producer_typed(pg_dsn: str) -> None:
    """PIN of today's observable: a capped actor's advisory lock, held by an
    open (actor-shaped) transaction, makes every OTHER producer of that actor
    raise the typed MaxPendingLockTimeoutError after the 5 s budget.

    Contract being pinned: while a transactional consumer's actor body runs
    inside its uncommitted transaction (worker/_consumer.py runs the actor
    inside transaction_conn.transaction() with timeout=None by default —
    settings.py default_start_to_close), a sub-enqueue's max_pending
    advisory lock spans that whole transaction ("the advisory locks then
    span that caller's transaction", backend/_enqueue.py), so another
    producer's enqueue waits out the DEFAULT_MAX_PENDING_LOCK_TIMEOUT_MS
    budget and fails typed — for the actor's entire runtime. A deliberate
    decoupling of sub-enqueue locks from the actor transaction must show up
    here as a transition (the other producer's enqueue succeeding).
    """
    conn_a, schema = await _fresh_schema(pg_dsn)
    conn_b = await asyncpg.connect(pg_dsn, command_timeout=30.0)
    sql = render_sql(schema)
    clock = SystemClock()
    try:
        args_a = dataclasses.replace(make_enqueue_args(actor="capped_actor"), max_pending=5)
        args_b = dataclasses.replace(make_enqueue_args(actor="capped_actor"), max_pending=5)
        async with conn_a.transaction():
            await _enqueue_on_conn(conn_a, sql, schema, clock, args_a)
            with pytest.raises(MaxPendingLockTimeoutError) as excinfo:
                await asyncio.wait_for(
                    _enqueue_on_conn(conn_b, sql, schema, clock, args_b), timeout=10.0
                )
        assert "max_pending" in repr(excinfo.value), (
            "Contract: the bounded advisory-lock exhaustion must surface as the typed "
            f"MaxPendingLockTimeoutError; got {excinfo.value!r}"
        )
    finally:
        with contextlib.suppress(Exception):
            await conn_b.close()
        with contextlib.suppress(Exception):
            await conn_a.close()
        await _drop_schema_quietly(pg_dsn, schema)


async def test_open_tx_idempotency_token_must_not_block_other_client_unbounded(pg_dsn: str) -> None:
    """PIN of today's observable: enqueueing the same
    (idempotency_scope, idempotency_key) from another client while the
    holder's transaction is still open resolves within a bounded window —
    the typed IdempotencyKeyLockTimeoutError at the 5 s idempotency
    lock_timeout budget.

    Contract being pinned: an unbounded actor body
    (default_start_to_close=None) must not hold cross-actor enqueue
    serialization — the same-key enqueue from a second client carries a
    documented bound: the savepoint-scoped lock_timeout around the
    speculative-token INSERT (backend/_enqueue.py's idempotency arm,
    DEFAULT_IDEMPOTENCY_LOCK_TIMEOUT_MS = 5 s) expires and raises the
    typed refusal. The window (8 s) sits beyond that budget so the
    designed refusal lands inside it and is asserted as the pass shape;
    the elapsed-margin discrimination fails only when the wait survives to
    the window itself — the only thing that ended such a wait is the
    test's own wait_for, proving the enqueue carries no bound of its own.
    A deliberate decoupling of the token wait from the holder's
    transaction must show up here as a transition (the second client's
    enqueue succeeding). Every wait in this test is bounded by
    asyncio.wait_for so a regression can never hang the suite.
    """
    conn_a, schema = await _fresh_schema(pg_dsn)
    conn_b = await asyncpg.connect(pg_dsn, command_timeout=30.0)
    sql = render_sql(schema)
    clock = SystemClock()
    key = f"actor-tx-{new_base62()}".lower()
    try:
        async with conn_a.transaction():
            await _enqueue_on_conn(
                conn_a, sql, schema, clock, make_enqueue_args(idempotency_key=key)
            )
            started = time.monotonic()
            try:
                await asyncio.wait_for(
                    _enqueue_on_conn(
                        conn_b, sql, schema, clock, make_enqueue_args(idempotency_key=key)
                    ),
                    timeout=_TEST_BOUND_S,
                )
            except IdempotencyKeyLockTimeoutError as refusal:
                assert "idempotency_key" in repr(refusal), (
                    "Contract: the bounded speculative-token wait must surface as the "
                    f"typed IdempotencyKeyLockTimeoutError; got {refusal!r}"
                )
            except TimeoutError as exc:
                elapsed = time.monotonic() - started
                if elapsed >= _UNBOUNDED_MARGIN_S:
                    pytest.fail(
                        "Contract: a same-key idempotency enqueue from another client must "
                        "resolve within a bounded window while the holder's transaction "
                        "is open — an unbounded actor body must not hold cross-actor "
                        "enqueue serialization (a documented bound or decoupling). "
                        "Regression shape: conn B's INSERT ... ON CONFLICT "
                        "(idempotency_scope, idempotency_key) DO NOTHING (backend/"
                        "_sql_templates.py speculative-token arbiter) blocked on conn "
                        "A's uncommitted row with NO bound — not the savepoint-scoped "
                        "lock_timeout (backend/_enqueue.py, 5 s default), not the "
                        "client-side wait — and was still blocked at the test's own "
                        f"{elapsed:.1f} s bound ({exc!r}) while A's transaction stayed "
                        "open; the only thing that ended the wait was this test's "
                        "wait_for, proving the enqueue carries no bound of its own"
                    )
    finally:
        with contextlib.suppress(Exception):
            await conn_b.close()
        with contextlib.suppress(Exception):
            await conn_a.close()
        await _drop_schema_quietly(pg_dsn, schema)


async def test_idempotency_enqueue_blocked_until_holder_tx_commits_then_dedups(pg_dsn: str) -> None:
    """PIN of today's mechanism: the same-key racer blocks on the holder's
    UNCOMMITTED speculative token and is released exactly at the holder's
    COMMIT, then dedup-returns the winner's row.

    Contract being pinned: the dedup outcome itself is correct — once the
    holder's transaction commits, the blocked racer's ON CONFLICT fires and
    the follow-up SELECT hands back the winner's row. This pin documents the
    mechanism the RED twin above attacks: the block ends only at the
    holder's transaction resolution, which for an unbounded actor
    transaction is the actor's whole runtime.
    """
    conn_a, schema = await _fresh_schema(pg_dsn)
    conn_b = await asyncpg.connect(pg_dsn, command_timeout=30.0)
    sql = render_sql(schema)
    clock = SystemClock()
    key = f"commit-release-{new_base62()}".lower()
    b_task: asyncio.Task[object] | None = None
    try:
        async with conn_a.transaction():
            row_a = await _enqueue_on_conn(
                conn_a, sql, schema, clock, make_enqueue_args(idempotency_key=key)
            )
            b_task = asyncio.create_task(
                _enqueue_on_conn(conn_b, sql, schema, clock, make_enqueue_args(idempotency_key=key))
            )
            try:
                await asyncio.wait_for(asyncio.shield(b_task), timeout=1.5)
                early = True
            except TimeoutError:
                early = False
            assert not early, (
                "Contract: the racer must still be blocked while the holder's "
                "speculative token is uncommitted — completing before the holder's "
                "commit would mean the token does not serialize same-key enqueues at all"
            )
        row_b = await asyncio.wait_for(b_task, timeout=5.0)
        assert getattr(row_b, "id", None) == row_a.id, (
            "Contract: after the holder's COMMIT the blocked racer must dedup-return "
            f"the winner's row (expected id {row_a.id}, got {row_b!r})"
        )
    finally:
        if b_task is not None and not b_task.done():
            b_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await b_task
        with contextlib.suppress(Exception):
            await conn_b.close()
        with contextlib.suppress(Exception):
            await conn_a.close()
        await _drop_schema_quietly(pg_dsn, schema)

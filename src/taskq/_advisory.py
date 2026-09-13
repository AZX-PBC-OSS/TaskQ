"""Two-tier bounded advisory-lock acquire — zero-dependency leaf module.

Shared by the two single-enqueue serialization sites (max_pending
admission and unique_for single-flight in
``taskq.backend._enqueue``); kept at a neutral top-level path (the
``_json``/``_scope`` convention) so both sites import one implementation
instead of carrying two copies of the contention machinery.

The acquire is two-tier because the two failure regimes need different
owners for the wait bound:

- Uncontended (the overwhelmingly common case): one
  ``pg_try_advisory_xact_lock`` statement. Round-trip count on the happy
  path is exactly one — identical to the pre-bounded era, so the bound
  costs the common case nothing.
- Contended: a server-side bounded blocking acquire —
  ``pg_advisory_xact_lock`` queued by Postgres' own lock scheduler, which
  hands the lock to the next waiter in ~ms as each holder's transaction
  ends. MEASURED (N same-key racers, ~1.5 ms holder critical section, PG
  18): the server-side queue drains 128 racers in ~0.4 s with zero
  timeouts, while a client-side try-lock poll loop (5 ms->100 ms
  exponential, no jitter) took ~5.1 s with 17-59% of racers exhausting
  their budget — failed pollers sleep while the lock sits idle between
  poll waves, draining ~1-2 racers per 100 ms against the queue's ~1 per
  few ms. A poll only wins when the holder is black-holed, and the
  client-side backstop below covers that regime more cheaply.
"""

import asyncio

from asyncpg.exceptions import LockNotAvailableError

from taskq.backend._protocol import ConnLike

__all__ = [
    "DEFAULT_ADVISORY_LOCK_CLIENT_BACKSTOP_SLACK_S",
    "acquire_advisory_xact_lock_bounded",
]

#: Fast-path statement: returns bool (acquired or not) without queueing,
#: so an uncontended racer pays exactly one round trip. hashtextextended
#: keys follow the convention already used for the prune, archive-expiry,
#: and migration locks; a collision between two different lock keys costs
#: a little needless serialization and never correctness.
_ADVISORY_TRY_LOCK_SQL = "SELECT pg_try_advisory_xact_lock(hashtextextended($1, 0))"

#: Contended-tier statement: queues the caller behind the current holder
#: in Postgres' lock scheduler. Bounded by the ``lock_timeout`` GUC set
#: inside the surrounding savepoint (see the acquire below).
_ADVISORY_BLOCKING_LOCK_SQL = "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))"

#: Reads the session/transaction's current ``lock_timeout`` so the
#: contended tier can restore it exactly — never clobbers a caller-set
#: bound on a BYO transaction, never assumes the default is "0".
_LOCK_TIMEOUT_READ_SQL = "SELECT current_setting('lock_timeout')"

#: ``set_config(..., true)`` is SET LOCAL semantics with a bindable
#: parameter (utility ``SET`` statements cannot take extended-protocol
#: parameters), so the budget value never has to be interpolated into
#: SQL text.
_LOCK_TIMEOUT_SET_SQL = "SELECT set_config('lock_timeout', $1, true)"

#: Extra seconds the client-side backstop waits past the budget (see the
#: acquire below). Must comfortably cover the contended tier's ~4 short
#: round trips around the blocking acquire plus the savepoint rollback
#: that the cancellation path issues, without converting legitimate
#: near-budget grants into client timeouts.
DEFAULT_ADVISORY_LOCK_CLIENT_BACKSTOP_SLACK_S: float = 0.5


async def acquire_advisory_xact_lock_bounded(
    conn: ConnLike,
    lock_key: str,
    *,
    timeout_ms: float,
) -> bool:
    """Acquire the transaction-scoped advisory lock *lock_key*, bounded by
    *timeout_ms*.

    Returns True once the lock is HELD for the rest of the transaction (a
    transaction-scoped advisory lock acquired inside the savepoint
    survives the savepoint's RELEASE); False when the budget expired
    first — the caller layers its own typed exhaustion error on the
    False, keeping each site's documented semantics. A raw driver error
    never surfaces from contention itself, only from non-contention
    failures (network, SQL) which propagate unchanged.

    ``timeout_ms <= 0`` waits indefinitely: one plain blocking acquire,
    no savepoint and no GUC statements — the ``lock_timeout`` GUC
    convention shared with migrate.py, and the documented opt-out for
    callers that want the pre-bound queueing behavior.

    Contended-tier mechanics (each step verified against live PG 18):

    1. ``SAVEPOINT`` (asyncpg's nested ``async with conn.transaction()``;
       on a transaction-less connection asyncpg opens a real short
       transaction instead — the lock then releases at its COMMIT,
       matching the bare-connection advisory-only semantics the try-lock
       fast path already has).
    2. Save the prior ``lock_timeout`` (``current_setting``), then
       ``set_config('lock_timeout', '<budget>ms', true)``.
    3. ``SELECT pg_advisory_xact_lock(...)`` — the bounded blocking wait.
       On timeout the statement raises SQLSTATE 55P03
       (:class:`asyncpg.exceptions.LockNotAvailableError`); the savepoint
       context manager rolls back, which (a) restores the transaction to
       a usable state — a raw statement error would otherwise leave
       "current transaction is aborted" behind — and (b) undoes the GUC
       set above. The exception is caught OUTSIDE the context manager and
       converted to the False return.
    4. On success, restore the saved ``lock_timeout`` BEFORE the
       savepoint's RELEASE: ``SET LOCAL``/``set_config(local=true)``
       effects are undone by ROLLBACK TO SAVEPOINT but PERSIST through
       RELEASE, so skipping the restore would leak the wait bound onto
       every later statement of the caller's transaction (and clobber a
       caller-set bound with the restore happening at all).

    Client-side backstop: the whole contended tier is wrapped in
    ``asyncio.wait_for(..., budget + slack)``. Why BOTH layers: the
    server-side timeout is precise (it fires at the budget even under
    client-side event-loop stalls, keeps the transaction usable via the
    savepoint rollback, and generates no cancel traffic on healthy
    pools), but it cannot fire if the server is UNREACHABLE — a network
    black hole where the acquire statement never returns. The backstop's
    cancellation triggers the same savepoint rollback (asyncpg resyncs
    the connection on cancellation; an advisory xact lock granted inside
    the savepoint is released by its ROLLBACK TO SAVEPOINT, so a cancel
    landing after the grant leaves no phantom holder), and the same
    False is returned. The slack keeps the backstop strictly behind the
    server-side timeout on any healthy network.
    """
    if await conn.fetchval(_ADVISORY_TRY_LOCK_SQL, lock_key):
        return True
    if timeout_ms <= 0:
        await conn.execute(_ADVISORY_BLOCKING_LOCK_SQL, lock_key)
        return True

    async def _contended_blocking_acquire() -> None:
        async with conn.transaction():
            prior = await conn.fetchval(_LOCK_TIMEOUT_READ_SQL)
            await conn.execute(_LOCK_TIMEOUT_SET_SQL, f"{round(timeout_ms)}ms")
            await conn.execute(_ADVISORY_BLOCKING_LOCK_SQL, lock_key)
            await conn.execute(_LOCK_TIMEOUT_SET_SQL, str(prior))

    try:
        await asyncio.wait_for(
            _contended_blocking_acquire(),
            timeout=timeout_ms / 1000.0 + DEFAULT_ADVISORY_LOCK_CLIENT_BACKSTOP_SLACK_S,
        )
    except (LockNotAvailableError, TimeoutError):
        return False
    return True

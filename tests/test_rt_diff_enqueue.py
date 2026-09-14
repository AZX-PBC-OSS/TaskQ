# Why: schema is a fixed test identifier, not user input; every value is $-bound.
"""Differential attacks on the enqueue dedup arms (unique_for / idempotency / singleton).

Each scenario runs identically against InMemoryBackend (FakeClock) and
PostgresBackend (real schema); the harness normalizes observables and
asserts equality.  Postgres is the contract source — a mirror that dedupes
(or admits) differently certifies code whose production behavior diverges.
"""

from __future__ import annotations

import pytest

from taskq._ids import new_job_id
from taskq.backend._protocol import EnqueueArgs, IdempotencyKey, IdentityKey
from taskq.exceptions import SingletonCollisionError
from tests.test_rt_diff_harness import DiffSide, assert_mirror, run_differential

pytestmark = pytest.mark.integration


# ── unique_for ─────────────────────────────────────────────────────────


async def _unique_for_live_window_hit(side: DiffSide) -> None:
    row1 = await side.enqueue("j1", identity_key="id-a", unique_for_s=60.0)
    side.record("first_owner", side.token_of(row1.id))
    row2 = await side.enqueue("j2", identity_key="id-a", unique_for_s=60.0)
    side.record("second_owner", side.token_of(row2.id))


async def test_diff_unique_for_live_window_dedupes(pg_dsn: str) -> None:
    """A live-window unique_for hit must return the existing row on both backends."""
    mem, pg = await run_differential(_unique_for_live_window_hit, pg_dsn=pg_dsn)
    assert_mirror(
        "unique_for preflight hit inside the window returns the existing row "
        "(no second row written)",
        mem,
        pg,
    )
    assert pg["records"] == {"first_owner": "j1", "second_owner": "j1"}


async def _unique_for_expired_window(side: DiffSide) -> None:
    await side.enqueue("j1", identity_key="id-a", unique_for_s=60.0)
    # Drive both clocks past the window: memory rewrites created_at, PG binds
    # clock_timestamp() arithmetic — the same logical instant per domain.
    await side.mutate("j1", created_ago_s=120.0)
    row2 = await side.enqueue("j2", identity_key="id-a", unique_for_s=60.0)
    side.record("second_owner", side.token_of(row2.id))


async def test_diff_unique_for_expired_window_admits(pg_dsn: str) -> None:
    """Once the unique_for window has elapsed, the same identity enqueues fresh."""
    mem, pg = await run_differential(_unique_for_expired_window, pg_dsn=pg_dsn)
    assert_mirror(
        "unique_for only dedupes inside its window; an expired window admits "
        "a new row for the same identity",
        mem,
        pg,
    )
    assert pg["records"] == {"second_owner": "j2"}
    assert pg["status_counts"] == {"pending": 2}


async def _unique_for_terminal_state_folded_in(side: DiffSide) -> None:
    await side.enqueue("j1", identity_key="id-a")
    await side.dispatch("w1", ["default"], limit=1)
    await side.mark_cancelled("j1", "w1")
    row2 = await side.enqueue(
        "j2", identity_key="id-a", unique_for_s=3600.0, unique_states=("cancelled",)
    )
    side.record("terminal_state_returns", side.token_of(row2.id))
    # And with the DEFAULT unique_states the terminal row must NOT dedupe:
    row3 = await side.enqueue("j3", identity_key="id-a", unique_for_s=3600.0)
    side.record("default_states_owner", side.token_of(row3.id))


async def test_diff_unique_for_terminal_state_set(pg_dsn: str) -> None:
    """A caller-configured unique_states set folding a terminal state dedupes
    against that terminal row; the default set does not."""
    mem, pg = await run_differential(_unique_for_terminal_state_folded_in, pg_dsn=pg_dsn)
    assert_mirror(
        "unique_states selects which rows a unique_for window dedupes against, "
        "terminal states included, identically on both backends",
        mem,
        pg,
    )
    assert pg["records"] == {"terminal_state_returns": "j1", "default_states_owner": "j3"}


# ── idempotency ────────────────────────────────────────────────────────


async def _idempotency_single_and_scope(side: DiffSide) -> None:
    row1 = await side.enqueue("j1", idempotency_key="key-1")
    side.record("first_owner", side.token_of(row1.id))
    row2 = await side.enqueue("j2", idempotency_key="key-1")
    side.record("same_scope_owner", side.token_of(row2.id))
    row3 = await side.enqueue("j3", idempotency_key="key-1", idempotency_scope="other")
    side.record("other_scope_owner", side.token_of(row3.id))


async def test_diff_idempotency_single_scoped(pg_dsn: str) -> None:
    """Idempotency dedupes per (scope, key): same pair returns the stored row,
    a different scope writes fresh."""
    mem, pg = await run_differential(_idempotency_single_and_scope, pg_dsn=pg_dsn)
    assert_mirror(
        "the idempotency arbiter is the (idempotency_scope, idempotency_key) pair on both backends",
        mem,
        pg,
    )
    assert pg["records"] == {
        "first_owner": "j1",
        "same_scope_owner": "j1",
        "other_scope_owner": "j3",
    }


async def _idempotency_batch(side: DiffSide) -> None:
    await side.enqueue("stored", idempotency_key="key-b")
    rows = await side.backend.enqueue_batch(
        [
            _batch_args(side, "in-batch-1", idempotency_key="key-c"),
            _batch_args(side, "in-batch-2", idempotency_key="key-c"),
            _batch_args(side, "stored-hit", idempotency_key="key-b"),
        ]
    )
    side.record(
        "batch_returns",
        [side.token_of(r.id) for r in rows],
    )


async def test_diff_idempotency_batch_dedupes(pg_dsn: str) -> None:
    """enqueue_batch's ON CONFLICT arm dedupes in-batch duplicates and stored
    pairs on PG; the mirror's per-item idempotency index must agree."""
    mem, pg = await run_differential(_idempotency_batch, pg_dsn=pg_dsn)
    assert_mirror(
        "batch idempotency: an in-batch duplicate and a stored pair both "
        "return the existing row, in call order",
        mem,
        pg,
    )
    assert pg["records"]["batch_returns"] == ["in-batch-1", "in-batch-1", "stored"]
    assert pg["status_counts"] == {"pending": 2}


def _batch_args(side: DiffSide, token: str, **kwargs: str | None) -> EnqueueArgs:
    """Build one batch EnqueueArgs in the side's clock domain (scheduled past).

    Registered eagerly so ``token_of`` resolves the RETURNED row (a dedup hit
    returns the earlier token's row instead of writing a new one).
    """
    args = EnqueueArgs(
        id=new_job_id(),
        actor="test_actor",
        queue="default",
        payload={"value": 1},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=side.ts(-1.0),
        idempotency_key=(IdempotencyKey(idem) if (idem := kwargs.get("idempotency_key")) else None),
        identity_key=IdentityKey(ident) if (ident := kwargs.get("identity_key")) else None,
    )
    side.register_job_id(token, args.id)
    return args


# ── singleton ──────────────────────────────────────────────────────────


async def _singleton_collision(side: DiffSide) -> None:
    await side.enqueue("j1", metadata={"singleton": True})
    try:
        await side.enqueue("j2", metadata={"singleton": True})
        side.record("second_enqueue", "admitted")
    except SingletonCollisionError as exc:
        side.record(
            "second_enqueue",
            ("SingletonCollisionError", exc.actor, exc.blocking_job_id is not None),
        )


async def test_diff_singleton_collision_refusal(pg_dsn: str) -> None:
    """A second live singleton for the same actor is refused identically."""
    mem, pg = await run_differential(_singleton_collision, pg_dsn=pg_dsn)
    assert_mirror(
        "the singleton preflight refuses a second live singleton with the same "
        "typed error and blocking-job attribution on both backends",
        mem,
        pg,
    )
    assert pg["records"]["second_enqueue"] == ["SingletonCollisionError", "test_actor", True]
    assert pg["status_counts"] == {"pending": 1}


async def _singleton_terminal_then_reenqueue(side: DiffSide) -> None:
    await side.enqueue("j1", metadata={"singleton": True})
    await side.dispatch("w1", ["default"], limit=1)
    await side.mark_cancelled("j1", "w1")
    row2 = await side.enqueue("j2", metadata={"singleton": True})
    side.record("after_terminal_owner", side.token_of(row2.id))


async def test_diff_singleton_allows_after_terminal(pg_dsn: str) -> None:
    """A terminal singleton no longer blocks: the next singleton enqueues fresh."""
    mem, pg = await run_differential(_singleton_terminal_then_reenqueue, pg_dsn=pg_dsn)
    assert_mirror(
        "the singleton preflight only sees pending/scheduled/running rows; a "
        "terminal singleton admits the next enqueue",
        mem,
        pg,
    )
    assert pg["records"] == {"after_terminal_owner": "j2"}
    assert pg["status_counts"] == {"cancelled": 1, "pending": 1}


async def _same_explicit_id_reinsert(side: DiffSide) -> None:
    first = await side.enqueue("j1", payload={"v": 1})
    try:
        await side.enqueue("j2", payload={"v": 2}, explicit_id=first.id)
        side.record("reinsert", "admitted")
    except Exception as exc:  # Why: the differential records the typed outcome; the exception type IS the observable.
        side.record("reinsert", type(exc).__name__)


async def test_diff_same_explicit_id_reinsert(pg_dsn: str) -> None:
    """Re-enqueueing an existing job id must fail identically on both backends.

    PG's jobs_pkey raises a typed unique violation; the mirror must not
    silently overwrite a live row (false confidence: code validated against
    the mirror corrupts on PG).
    """
    mem, pg = await run_differential(_same_explicit_id_reinsert, pg_dsn=pg_dsn)
    assert_mirror(
        "an INSERT carrying an existing job id is a constraint violation on "
        "PG; the mirror must refuse it just as loudly, never overwrite",
        mem,
        pg,
    )

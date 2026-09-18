# ruff: noqa: S608  # Why: schema is fixture-derived (module_pg_schema), not user input; every value is $-bound.

"""Red-team attacks on the reclaim sweep's heartbeat arm.

The settled direction is ENFORCEMENT (the earlier stack direction —
refusal at the enqueue boundary, reclaim stays lease-based — was
superseded): ``_SWEEP_1_SQL`` gained a second, disjoint
eligibility arm (``src/taskq/backend/_sweeps.py``, the ``heartbeat_arm``
CTE) that reclaims a running job whose holder has been silent past the
row's per-job ``heartbeat_timeout`` while its lock lease is still valid,
served by the partial index ``jobs_running_heartbeat_deadline_idx``
(migration 01.00.10_01) and pinned by
``tests/test_heartbeat_timeout_enforced.py`` and
``tests/test_index_audit.py``.

This file attacks what those pins do not reach. Verdicts expected at
commit time (the reds are the deliverable — they stay red until fixed):

* RED ``test_heartbeat_reclaim_attempt_row_must_not_claim_the_lock_expired``
  — the batched ``job_attempts`` INSERT hardcodes
  ``'lock expired before worker reported terminal state'`` for BOTH arms,
  so the audit row for a heartbeat reclaim asserts a lock expiry that the
  sweep itself disproved when it selected the row
  (``lock_expires_at >= statement_timestamp()``).
* RED ``test_twin_mirrors_the_sqls_past_beat_conjunct`` — the in-memory
  twin's heartbeat arm (``src/taskq/testing/_sweeps.py``) omits the SQL's
  ``last_heartbeat_at < statement_timestamp()`` conjunct, so a row the SQL
  provably never reclaims (a future-stamped beat plus a degenerate
  negative timeout — direct-SQL-reachable, like the NULL-beat state the
  twin does guard) is reclaimed by the twin. Constitution line 99: the
  twin is observably equivalent at every seam.
* ``test_ops_footgun_registry_names_the_inversion_trap`` — the
  lease-inversion trap (a ``heartbeat_timeout`` at or
  above the fleet's lock lease can never govern: the lease arm requires
  the lease still valid, so the lease deadline always fires first)
  silently no-ops the knob. ``docs/guides/ops.md``'s footgun registry
  names the trap next to the sibling lower-bound row, and with nothing
  at enqueue, dispatch, or sweep warning on the inversion, that row is
  the only operator-facing notice -- the pin holds it in place.

Every other test here is an attempt at refutation: if it stays green it
is a pin of a contract the author's pins leave uncovered (NULL knob /
unstamped beat, the deadline boundary either side, both-arms rows, the
cancel carve-out on the heartbeat arm's own deadline, the mid-write race
the arm's SKIP LOCKED must tolerate, per-arm batch limits, oldest-first
drain) on PG and on the twin.
"""

from __future__ import annotations

import json
from dataclasses import replace as dataclass_replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import CancelPhase, JobId
from taskq.backend.postgres import PostgresBackend
from taskq.testing.clock import FakeClock
from taskq.testing.fixtures import ModulePgSchema
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.jobs import make_job_row
from taskq.testing.pg import create_running_job, create_worker

_OPS_MD = Path(__file__).resolve().parents[1] / "docs" / "guides" / "ops.md"

#: Zero graces everywhere, the sibling file's convention: the cancel
#: carve-out's flat extra is then exactly its 60-second safety margin, so
#: deadline-vs-margin arithmetic is readable in the seeds below.
_GRACE = timedelta(seconds=0)

#: The in-memory twin's deterministic clock start (same convention as
#: tests/test_heartbeat_timeout_enforced.py's ``_TWIN_START``).
_TWIN_START = datetime(2025, 1, 1, tzinfo=UTC)

#: The heartbeat arm's flat cancel safety margin at zero graces — the
#: ``interval '60 seconds'`` literal in ``_SWEEP_1_SQL``'s carve-outs —
#: stated as a constant so the seed arithmetic in the carve-out tests
#: reads against the same number the SQL uses.
_CANCEL_MARGIN_SECONDS = 60


# ── PG seeding and read-back helpers ─────────────────────────────────


async def _seed_hb_running_job(
    conn: asyncpg.Connection,
    schema: str,
    worker_id: UUID,
    *,
    heartbeat_age: timedelta | None,
    heartbeat_timeout: timedelta | None,
    lease_expires_in: timedelta,
    cancel_phase: int = 0,
    max_attempts: int = 3,
    attempt: int = 1,
) -> UUID:
    """Seed one running job with explicit heartbeat columns.

    ``heartbeat_age`` is the age of ``last_heartbeat_at`` at seed time (a
    NEGATIVE value stamps a future beat — the direct-SQL state that
    discriminates the SQL's ``last_heartbeat_at < statement_timestamp()``
    conjunct); ``None`` stamps NULL (the direct-SQL unstamped state the
    SQL's comment says must "wait for its lease"). Both timestamps are
    database-written in the seed statement, so the sweep's
    ``statement_timestamp()`` compares against the same clock domain.
    """
    job_id = await create_running_job(
        conn,
        schema,
        worker_id,
        lock_expires_at=datetime.now(UTC) + lease_expires_in,
        cancel_phase=cancel_phase,
        cancel_requested_at=datetime.now(UTC) if cancel_phase else None,
        max_attempts=max_attempts,
        attempt=attempt,
        with_events=False,
    )
    await conn.execute(
        f'UPDATE "{schema}".jobs '
        "SET heartbeat_timeout = $2::interval, "
        "    last_heartbeat_at = CASE WHEN $3::interval IS NULL THEN NULL "
        "                            ELSE clock_timestamp() - $3::interval END "
        "WHERE id = $1",
        job_id,
        heartbeat_timeout,
        heartbeat_age,
    )
    return job_id


async def _job_status(conn: asyncpg.Connection, schema: str, job_id: UUID) -> str:
    value = await conn.fetchval(f'SELECT status::text FROM "{schema}".jobs WHERE id = $1', job_id)
    return str(value)


async def _latest_reclaim_detail(
    conn: asyncpg.Connection, schema: str, job_id: UUID
) -> dict[str, object]:
    """The job's newest state_change event detail, decoded.

    ``detail::text`` plus ``json.loads`` (the sibling file's convention)
    so the read works whether or not the connection has a jsonb codec.
    """
    raw = await conn.fetchval(
        f'SELECT detail::text FROM "{schema}".job_events '
        "WHERE job_id = $1 AND kind = 'state_change' ORDER BY id DESC LIMIT 1",
        job_id,
    )
    assert raw is not None, f"no state_change event written for job {job_id}"
    decoded: dict[str, object] = json.loads(str(raw))
    return decoded


async def _attempt_count(conn: asyncpg.Connection, schema: str, job_id: UUID) -> int:
    value = await conn.fetchval(
        f'SELECT count(*) FROM "{schema}".job_attempts WHERE job_id = $1', job_id
    )
    return int(value)


async def _event_count(conn: asyncpg.Connection, schema: str, job_id: UUID) -> int:
    value = await conn.fetchval(
        f'SELECT count(*) FROM "{schema}".job_events WHERE job_id = $1', job_id
    )
    return int(value)


# ── in-memory twin helpers ───────────────────────────────────────────


def _twin_backend() -> InMemoryBackend:
    return InMemoryBackend(
        clock=FakeClock(_TWIN_START),
        cancellation_grace_period=_GRACE,
        cleanup_grace_period=_GRACE,
    )


def _twin_running_row(
    backend: InMemoryBackend,
    *,
    heartbeat_at: datetime | None,
    heartbeat_timeout: timedelta | None,
    lease: datetime,
    cancel_phase: int = 0,
    max_attempts: int = 3,
    attempt: int = 1,
) -> JobId:
    """Seed one running row in the twin's private store (the family's
    test seam — tests/test_heartbeat_timeout_enforced.py and
    tests/test_in_memory_backend.py use the same)."""
    row = make_job_row(
        heartbeat_timeout=heartbeat_timeout,
        cancel_phase=cancel_phase,
        max_attempts=max_attempts,
        attempt=attempt,
    )
    running = dataclass_replace(
        row,
        status="running",
        locked_by_worker=backend._worker_id,
        lock_expires_at=lease,
        last_heartbeat_at=heartbeat_at,
        # Parity with _seed_hb_running_job: a phase-carrying row is
        # seeded with its request timestamp, so the preserved-column
        # assertions below read a real audit value, not the None the
        # bare make_job_row default leaves.
        cancel_requested_at=_TWIN_START if cancel_phase else None,
    )
    backend._jobs[running.id] = running
    return running.id


async def _twin_reclaim_detail(backend: InMemoryBackend, job_id: JobId) -> dict[str, object]:
    events = [e for e in await backend.get_events(job_id) if e.kind == "state_change"]
    assert events, f"the twin wrote no state_change event for job {job_id}"
    detail: dict[str, object] = events[-1].detail
    return detail


# ── PG: predicate edge pins ──────────────────────────────────────────


@pytest.mark.integration
async def test_null_knob_and_unstamped_beat_are_never_heartbeat_eligible(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """A row with no ``heartbeat_timeout`` — however ancient its beat —
    and a row with the knob but no stamped beat must both stay running
    while their lease is valid: the arm's first conjunct is
    ``heartbeat_timeout IS NOT NULL``, and NULL ``last_heartbeat_at`` is
    never eligible (NULL + interval is NULL, so the row waits for its
    lease — ``_SWEEP_1_SQL``'s comment states this as the contract).

    The sibling file's rows all carry the knob AND a stamped beat, so
    neither half of this never-match pin exists there.
    """
    schema = module_pg_schema.schema_name
    worker_id = new_uuid()
    await create_worker(clean_pg_conn, schema, worker_id)
    no_knob = await _seed_hb_running_job(
        clean_pg_conn,
        schema,
        worker_id,
        heartbeat_age=timedelta(hours=1),
        heartbeat_timeout=None,
        lease_expires_in=timedelta(hours=1),
    )
    no_beat = await _seed_hb_running_job(
        clean_pg_conn,
        schema,
        worker_id,
        heartbeat_age=None,
        heartbeat_timeout=timedelta(seconds=30),
        lease_expires_in=timedelta(hours=1),
    )
    control = await _seed_hb_running_job(
        clean_pg_conn,
        schema,
        worker_id,
        heartbeat_age=timedelta(hours=1),
        heartbeat_timeout=timedelta(seconds=30),
        lease_expires_in=timedelta(hours=1),
    )

    count = await PostgresBackend.sweep_expired_locks(clean_pg_conn, _GRACE, _GRACE, schema=schema)

    assert count == 1, (
        f"the sweep reclaimed {count} rows; only the control (knob set, beat "
        "1h stale, lease valid) is eligible — a NULL-knob row with an ancient "
        "beat or a knob-carrying row with no beat must not match any arm."
    )
    assert await _job_status(clean_pg_conn, schema, control) != "running", (
        "the control row (knob set, beat 1h stale, lease valid) was not "
        "reclaimed — the sweep did not run its heartbeat arm at all, so the "
        "two never-match assertions above prove nothing."
    )
    assert await _job_status(clean_pg_conn, schema, no_knob) == "running", (
        "a running job with heartbeat_timeout NULL was reclaimed while its "
        "lease was still valid — the heartbeat arm requires the knob."
    )
    assert await _job_status(clean_pg_conn, schema, no_beat) == "running", (
        "a running job whose last_heartbeat_at is NULL was reclaimed while "
        "its lease was still valid — NULL + interval is NULL, so the row "
        "must wait for its lease (the _SWEEP_1_SQL comment's contract)."
    )
    for job_id in (no_knob, no_beat):
        assert await _attempt_count(clean_pg_conn, schema, job_id) == 0
        assert await _event_count(clean_pg_conn, schema, job_id) == 0


@pytest.mark.integration
async def test_deadline_boundary_just_short_just_past(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """Silence age just SHORT of the timeout must not reclaim; just PAST
    it must — with ``cancel_phase = 0`` so no carve-out applies.

    The 5-second margins are comfortably above the seed-to-sweep
    round-trip latency, so both sides are deterministic. The just-past
    row is also the discriminator for an accidentally UNCONDITIONAL
    cancel carve-out: 5s past the deadline is far inside the 60s margin,
    so a rewrite that drops the ``cancel_phase = 0 OR`` disjunct leaves
    this row running while the sibling file's 1h-stale row (past every
    margin) stays green — exactly the gap a red-team pass exists to hold.
    """
    schema = module_pg_schema.schema_name
    worker_id = new_uuid()
    await create_worker(clean_pg_conn, schema, worker_id)
    just_short = await _seed_hb_running_job(
        clean_pg_conn,
        schema,
        worker_id,
        heartbeat_age=timedelta(seconds=25),
        heartbeat_timeout=timedelta(seconds=30),
        lease_expires_in=timedelta(hours=1),
    )
    just_past = await _seed_hb_running_job(
        clean_pg_conn,
        schema,
        worker_id,
        heartbeat_age=timedelta(seconds=35),
        heartbeat_timeout=timedelta(seconds=30),
        lease_expires_in=timedelta(hours=1),
        max_attempts=1,
        attempt=1,
    )

    count = await PostgresBackend.sweep_expired_locks(clean_pg_conn, _GRACE, _GRACE, schema=schema)

    assert count == 1, f"only the just-past row is eligible, got {count}"
    assert await _job_status(clean_pg_conn, schema, just_short) == "running", (
        "a holder silent 5s SHORT of its heartbeat_timeout was reclaimed — "
        "the arm's deadline comparison must be strict: silence must run "
        "PAST the timeout, not merely approach it."
    )
    assert await _job_status(clean_pg_conn, schema, just_past) == "crashed", (
        "a holder silent 5s past its heartbeat_timeout (cancel_phase=0, so "
        "no carve-out) was not reclaimed on the heartbeat arm — the "
        "carve-out must apply only to rows with a cancel in flight."
    )
    detail = await _latest_reclaim_detail(clean_pg_conn, schema, just_past)
    assert detail.get("reason") == "lock_expired", (
        f"the heartbeat reclaim must ride the crash-reclaim outbox channel (detail={detail!r})."
    )
    assert detail.get("cause") == "heartbeat_timeout", (
        f"the reclaim event must name the deadline that fired (detail={detail!r})."
    )


@pytest.mark.integration
async def test_row_eligible_for_both_arms_is_reclaimed_once_by_the_lease_arm(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """A row that is BOTH lease-expired and heartbeat-stale is owned by
    the lease arm alone — the disjointness the heartbeat arm's
    ``lock_expires_at >= statement_timestamp()`` exclusion buys, and the
    reason UNION ALL is safe: a row reaching both arms hits the batched
    ``job_attempts`` INSERT twice and violates its (job_id, attempt)
    PRIMARY KEY, a non-transient error that tears down the leader.

    The sibling file's lease-expired row carries a FRESH beat, so the
    both-eligible shape is unpinned there.
    """
    schema = module_pg_schema.schema_name
    worker_id = new_uuid()
    await create_worker(clean_pg_conn, schema, worker_id)
    both = await _seed_hb_running_job(
        clean_pg_conn,
        schema,
        worker_id,
        heartbeat_age=timedelta(hours=1),
        heartbeat_timeout=timedelta(seconds=30),
        lease_expires_in=timedelta(seconds=-10),
    )

    count = await PostgresBackend.sweep_expired_locks(clean_pg_conn, _GRACE, _GRACE, schema=schema)

    assert count == 1, (
        f"a both-eligible row was visited by both arms (count={count}) — the "
        "arms overlap, and the next batched attempt INSERT violates "
        "job_attempts' PRIMARY KEY (job_id, attempt)."
    )
    assert await _attempt_count(clean_pg_conn, schema, both) == 1, (
        "the both-eligible row must carry exactly one job_attempts row"
    )
    assert await _event_count(clean_pg_conn, schema, both) == 1, (
        "the both-eligible row must carry exactly one job_events row"
    )
    detail = await _latest_reclaim_detail(clean_pg_conn, schema, both)
    assert detail.get("cause") == "lock_expired", (
        f"the lease arm owns rows whose lease has expired, whichever other "
        f"deadline is also past (detail={detail!r}) — the heartbeat arm "
        "requires the lease still valid, by construction disjoint."
    )


@pytest.mark.integration
async def test_cancel_carve_out_on_the_heartbeat_arm_rides_the_heartbeat_deadline(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """The heartbeat arm's cancel carve-out must apply the grace ladder
    to the HEARTBEAT deadline expression
    (``last_heartbeat_at + heartbeat_timeout < now - graces - 60s``), not
    to ``lock_expires_at``.

    Every row here carries a lease an hour in the future, so only the
    heartbeat arm can reach them at all: a copy-paste of the lease arm's
    ``lock_expires_at``-based carve-out would leave EVERY row ineligible
    (the lease is never past the margin) and the two deep-past rows would
    stop being reclaimed — the discriminator this test exists to hold.
    Both deep-past rows land 'cancelled' (#238: operator intent outranks
    the retry budget), with the cancel columns preserved as the audit
    trail of the honored request.
    """
    schema = module_pg_schema.schema_name
    worker_id = new_uuid()
    await create_worker(clean_pg_conn, schema, worker_id)
    timeout = timedelta(seconds=30)
    # Deadline 20s INSIDE the flat 60s margin at zero graces.
    deadline_past_inside = timedelta(seconds=_CANCEL_MARGIN_SECONDS - 20)
    # Deadline 110s BEYOND the same margin.
    deadline_past_deep = timedelta(seconds=_CANCEL_MARGIN_SECONDS + 110)
    inside_margin = await _seed_hb_running_job(
        clean_pg_conn,
        schema,
        worker_id,
        heartbeat_age=timeout + deadline_past_inside,
        heartbeat_timeout=timeout,
        lease_expires_in=timedelta(hours=1),
        cancel_phase=1,
    )
    deep_exhausted = await _seed_hb_running_job(
        clean_pg_conn,
        schema,
        worker_id,
        heartbeat_age=timeout + deadline_past_deep,
        heartbeat_timeout=timeout,
        lease_expires_in=timedelta(hours=1),
        cancel_phase=1,
        max_attempts=1,
        attempt=1,
    )
    deep_retryable = await _seed_hb_running_job(
        clean_pg_conn,
        schema,
        worker_id,
        heartbeat_age=timeout + deadline_past_deep,
        heartbeat_timeout=timeout,
        lease_expires_in=timedelta(hours=1),
        cancel_phase=1,
        max_attempts=3,
        attempt=1,
    )

    count = await PostgresBackend.sweep_expired_locks(clean_pg_conn, _GRACE, _GRACE, schema=schema)

    assert count == 2, (
        f"expected the two deadline-past-cancel rows reclaimed and the "
        f"inside-margin row left for the cancellation protocol, got {count}"
    )
    assert await _job_status(clean_pg_conn, schema, inside_margin) == "running", (
        "a heartbeat-stale row with a cancel in flight, whose deadline is "
        "only 40s past, was reclaimed — the heartbeat arm's carve-out must "
        "give the cancellation protocol its cancel_grace + cleanup_grace + "
        "60s headroom past the HEARTBEAT deadline."
    )
    assert await _job_status(clean_pg_conn, schema, deep_exhausted) == "cancelled", (
        "an exhausted heartbeat-stale row with a cancel in flight must land "
        "on 'cancelled' — the caller's explicit request is the honest "
        "terminal label, on the heartbeat arm exactly as on the lease arm."
    )
    assert await _job_status(clean_pg_conn, schema, deep_retryable) == "cancelled", (
        "a retryable heartbeat-stale row with a cancel in flight must land "
        "on 'cancelled' too: operator intent outranks the retry budget "
        "(#238): the pre-fix budget-first CASE re-pended this row 'pending' "
        "and wiped the operator's cancel, addressed to a holder the sweep "
        "itself had just declared dead."
    )
    detail = await _latest_reclaim_detail(clean_pg_conn, schema, deep_exhausted)
    assert detail.get("cause") == "heartbeat_timeout", (
        f"the carve-out path is still the heartbeat arm's reclaim — the "
        f"event must name it (detail={detail!r})."
    )
    retry_row = await clean_pg_conn.fetchrow(
        f'SELECT cancel_phase, cancel_requested_at FROM "{schema}".jobs WHERE id = $1',
        deep_retryable,
    )
    assert retry_row is not None
    assert retry_row["cancel_phase"] == 1, (
        "the cancel arm preserves cancel_phase as the audit trail of the "
        "honored request, the same doctrine mark_cancelled carries"
    )
    assert retry_row["cancel_requested_at"] is not None, (
        "the cancel arm preserves cancel_requested_at as the audit trail of the honored request"
    )


@pytest.mark.integration
async def test_future_beat_is_not_heartbeat_eligible_even_with_degenerate_timeout(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """The arm's ``last_heartbeat_at < statement_timestamp()`` conjunct —
    the "necessary condition" the migration's header says is stated
    explicitly as the partial index's range bound — must hold even when
    the row-exact deadline arithmetic alone would admit the row: a
    FUTURE-stamped beat plus a degenerate NEGATIVE timeout (direct-SQL
    reachable only; enqueue refuses non-positive values) makes
    ``last_heartbeat_at + heartbeat_timeout`` sit in the past while the
    beat itself does not.

    This is the PG half of the twin parity red at the bottom of this
    file: the SQL does not reclaim this row, so the twin must not either.
    """
    schema = module_pg_schema.schema_name
    worker_id = new_uuid()
    await create_worker(clean_pg_conn, schema, worker_id)
    future_beat = await _seed_hb_running_job(
        clean_pg_conn,
        schema,
        worker_id,
        # Beat stamped 10s into the future; timeout -30s: the deadline
        # arithmetic says "20s past" but the beat is not in the past.
        heartbeat_age=timedelta(seconds=-10),
        heartbeat_timeout=timedelta(seconds=-30),
        lease_expires_in=timedelta(hours=1),
    )
    control = await _seed_hb_running_job(
        clean_pg_conn,
        schema,
        worker_id,
        heartbeat_age=timedelta(hours=1),
        heartbeat_timeout=timedelta(seconds=30),
        lease_expires_in=timedelta(hours=1),
    )

    count = await PostgresBackend.sweep_expired_locks(clean_pg_conn, _GRACE, _GRACE, schema=schema)

    assert count == 1, (
        f"only the control row is eligible (a future beat must not be "
        f"heartbeat-eligible however the deadline arithmetic reads), got {count}"
    )
    assert await _job_status(clean_pg_conn, schema, future_beat) == "running", (
        "a row whose last_heartbeat_at is stamped in the FUTURE was "
        "reclaimed by the heartbeat arm — the arm requires "
        "last_heartbeat_at < statement_timestamp() as its index range "
        "bound, and a beat that is not in the past is never holder silence."
    )
    assert await _job_status(clean_pg_conn, schema, control) != "running"


# ── PG: the mid-write race and the batch geometry ────────────────────


@pytest.mark.integration
async def test_mid_write_heartbeat_row_is_skipped_not_lost(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
    pg_dsn: str,
) -> None:
    """A heartbeat tick in flight holds the jobs row's row-lock (its
    UPDATE touches ``last_heartbeat_at``/``lock_expires_at``); the sweep
    must SKIP that row (FOR UPDATE SKIP LOCKED) without blocking past
    its statement_timeout, without writing anything for it, and without
    losing it — the next tick after the write aborts reclaims the same
    silence.

    The sibling files pin the arm's predicate but never hold a row lock
    across a sweep call; this is the race the sweep's batching must
    tolerate.
    """
    schema = module_pg_schema.schema_name
    worker_id = new_uuid()
    await create_worker(clean_pg_conn, schema, worker_id)
    job_id = await _seed_hb_running_job(
        clean_pg_conn,
        schema,
        worker_id,
        heartbeat_age=timedelta(hours=1),
        heartbeat_timeout=timedelta(seconds=30),
        lease_expires_in=timedelta(hours=1),
    )

    conn_a = await asyncpg.connect(pg_dsn)
    try:
        # The in-flight heartbeat tick: row lock held in an open
        # transaction on another connection.
        tx = conn_a.transaction()
        await tx.start()
        try:
            await conn_a.execute(f'SELECT id FROM "{schema}".jobs WHERE id = $1 FOR UPDATE', job_id)
            count = await PostgresBackend.sweep_expired_locks(
                clean_pg_conn, _GRACE, _GRACE, schema=schema
            )
            assert count == 0, (
                f"the sweep reclaimed {count} rows while the only eligible "
                "row's holder was mid-heartbeat-write — SKIP LOCKED must "
                "step over the locked row, not wait and not take it."
            )
            assert await _job_status(clean_pg_conn, schema, job_id) == "running"
            assert await _attempt_count(clean_pg_conn, schema, job_id) == 0, (
                "a skipped row must not get a job_attempts row"
            )
            assert await _event_count(clean_pg_conn, schema, job_id) == 0, (
                "a skipped row must not get a job_events row"
            )
        finally:
            # The tick aborts — the beat never lands, the silence stands.
            await tx.rollback()

        count_after = await PostgresBackend.sweep_expired_locks(
            clean_pg_conn, _GRACE, _GRACE, schema=schema
        )
        assert count_after == 1, (
            "the sweep lost the row it skipped: after the mid-write "
            "transaction aborted, the still-silent row must be reclaimed by "
            "the very next call."
        )
        detail = await _latest_reclaim_detail(clean_pg_conn, schema, job_id)
        assert detail.get("cause") == "heartbeat_timeout", (
            f"the post-race reclaim is the heartbeat arm's (detail={detail!r})"
        )
    finally:
        await conn_a.close()


@pytest.mark.integration
async def test_each_arm_carries_its_own_batch_limit(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """``batch_size = 1`` with one row eligible per arm must reclaim BOTH
    in one call — each arm's snap carries its own LIMIT (``_SWEEP_1_SQL``
    documents "at most 2 x batch_size"), so a rewrite that moves the
    LIMIT to the union'd snap halves the sweep's throughput per call
    silently.
    """
    schema = module_pg_schema.schema_name
    worker_id = new_uuid()
    await create_worker(clean_pg_conn, schema, worker_id)
    heartbeat_row = await _seed_hb_running_job(
        clean_pg_conn,
        schema,
        worker_id,
        heartbeat_age=timedelta(hours=1),
        heartbeat_timeout=timedelta(seconds=30),
        lease_expires_in=timedelta(hours=1),
    )
    lease_row = await _seed_hb_running_job(
        clean_pg_conn,
        schema,
        worker_id,
        heartbeat_age=timedelta(seconds=-10),
        heartbeat_timeout=timedelta(seconds=30),
        lease_expires_in=timedelta(seconds=-10),
    )

    count = await PostgresBackend.sweep_expired_locks(
        clean_pg_conn, _GRACE, _GRACE, schema=schema, batch_size=1
    )

    assert count == 2, (
        f"one heartbeat-eligible + one lease-eligible row at batch_size=1 "
        f"reclaimed {count} — the two arms must carry independent LIMITs, "
        "not a shared one over the union."
    )
    assert await _job_status(clean_pg_conn, schema, heartbeat_row) != "running"
    assert await _job_status(clean_pg_conn, schema, lease_row) != "running"


@pytest.mark.integration
async def test_heartbeat_arm_drains_oldest_silence_first(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """With ``batch_size = 1`` the heartbeat arm's ``ORDER BY
    last_heartbeat_at`` must drain oldest-silence-first: a lost ORDER BY
    still drains (counts match) but abandons the deterministic
    oldest-eligible-first order the sweep's comment documents, and this
    pin makes that loss visible one batch at a time.
    """
    schema = module_pg_schema.schema_name
    worker_id = new_uuid()
    await create_worker(clean_pg_conn, schema, worker_id)
    ages = (timedelta(minutes=10), timedelta(minutes=5), timedelta(minutes=1))
    job_ids = [
        await _seed_hb_running_job(
            clean_pg_conn,
            schema,
            worker_id,
            heartbeat_age=age,
            heartbeat_timeout=timedelta(seconds=30),
            lease_expires_in=timedelta(hours=1),
        )
        for age in ages
    ]

    reclaimed_so_far: list[UUID] = []
    for expected_id in job_ids:
        count = await PostgresBackend.sweep_expired_locks(
            clean_pg_conn, _GRACE, _GRACE, schema=schema, batch_size=1
        )
        assert count == 1, "each call at batch_size=1 must reclaim exactly one row"
        newly_reclaimed = [
            job_id
            for job_id in job_ids
            if job_id not in reclaimed_so_far
            and await _job_status(clean_pg_conn, schema, job_id) != "running"
        ]
        assert newly_reclaimed == [expected_id], (
            f"the drain must take the oldest silence first: expected "
            f"{expected_id} reclaimed this batch, got {newly_reclaimed!r} "
            "(already reclaimed: {reclaimed_so_far!r})"
        )
        reclaimed_so_far.append(expected_id)
    final = await PostgresBackend.sweep_expired_locks(
        clean_pg_conn, _GRACE, _GRACE, schema=schema, batch_size=1
    )
    assert final == 0, "the backlog must be fully drained"


# ── PG: the lease-inversion operational trap ───────────────────────────


@pytest.mark.integration
async def test_heartbeat_timeout_above_the_lease_never_governs_the_reclaim(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """The lease-inversion trap, pinned as it behaves today: a
    ``heartbeat_timeout`` at or above the fleet's lock lease can NEVER
    govern, because the heartbeat arm requires the lease still valid
    while the lease deadline (last beat + lease) always precedes the
    heartbeat deadline (last beat + timeout) — the lease arm fires
    first, every time.

    The row below carries a 1-hour heartbeat budget and a 30-second
    lease; its holder went silent 2 hours ago. The reclaim is the LEASE
    arm's (cause='lock_expired'), honestly naming the deadline that
    fired — but the caller's 1-hour patience budget silently no-oped,
    and nothing at enqueue, dispatch, or sweep says so (the companion
    docs red below attacks that gap). This pin keeps the behaviour
    itself honest and visible: whoever changes it knows the trap exists.
    """
    schema = module_pg_schema.schema_name
    worker_id = new_uuid()
    await create_worker(clean_pg_conn, schema, worker_id)
    # Beat 2h old; lease = last beat + 30s (expired 1h59m30s ago);
    # heartbeat deadline = last beat + 1h (past 1h ago) — both deadlines
    # are past, the lease's by an hour more.
    inverted = await _seed_hb_running_job(
        clean_pg_conn,
        schema,
        worker_id,
        heartbeat_age=timedelta(hours=2),
        heartbeat_timeout=timedelta(hours=1),
        lease_expires_in=timedelta(hours=-2) + timedelta(seconds=30),
    )

    count = await PostgresBackend.sweep_expired_locks(clean_pg_conn, _GRACE, _GRACE, schema=schema)

    assert count == 1
    detail = await _latest_reclaim_detail(clean_pg_conn, schema, inverted)
    assert detail.get("cause") == "lock_expired", (
        f"with heartbeat_timeout (1h) >= the lease (30s), the heartbeat arm "
        f"can never fire — the lease deadline always precedes it. The "
        f"reclaim must honestly name the lease (detail={detail!r}); the "
        "silent no-op of the caller's per-job budget is the inversion trap "
        "this pin documents."
    )


# ── PG RED: the attempt row's error_message ──────────────────────────


@pytest.mark.integration
async def test_heartbeat_reclaim_attempt_row_must_not_claim_the_lock_expired(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """RED at commit time (expected, the deliverable): the batched
    ``job_attempts`` INSERT (``_SWEEP_1_ATTEMPTS_BATCH_SQL``) hardcodes
    ``'lock expired before worker reported terminal state'`` for BOTH
    arms, so the audit row for a heartbeat-arm reclaim asserts a lock
    expiry that the sweep itself disproved when it selected the row —
    the heartbeat arm's predicate REQUIRES
    ``lock_expires_at >= statement_timestamp()``.

    The event surface is honest (reason rides the outbox channel,
    ``cause`` names the deadline); the attempt surface is the lie. An
    auditor reconciling ``job_attempts`` against ``jobs`` sees a job
    reclaimed with its lease an hour in the future and an attempt row
    claiming the lock expired — the exact "honest terminal label"
    standard the same sweep applies to the cancelled branch. The twin
    hardcodes the same string (``testing/_sweeps.py``), so the fix
    touches both.
    """
    schema = module_pg_schema.schema_name
    worker_id = new_uuid()
    await create_worker(clean_pg_conn, schema, worker_id)
    job_id = await _seed_hb_running_job(
        clean_pg_conn,
        schema,
        worker_id,
        heartbeat_age=timedelta(hours=1),
        heartbeat_timeout=timedelta(seconds=30),
        lease_expires_in=timedelta(hours=1),
        max_attempts=1,
        attempt=1,
    )

    count = await PostgresBackend.sweep_expired_locks(clean_pg_conn, _GRACE, _GRACE, schema=schema)
    assert count == 1

    attempt = await clean_pg_conn.fetchrow(
        f'SELECT outcome, error_class, error_message FROM "{schema}".job_attempts '
        "WHERE job_id = $1",
        job_id,
    )
    assert attempt is not None, "the heartbeat reclaim must write its job_attempts row"
    # The uncontroversial half (both hold today): the attempt IS a crash.
    assert attempt["outcome"] == "crashed"
    assert attempt["error_class"] == "WorkerCrashed"
    # The attack: the free-text message must not assert a lock expiry the
    # sweep disproved when it selected the row.
    assert "lock expired" not in str(attempt["error_message"]), (
        f"the attempt row for a HEARTBEAT reclaim claims "
        f"{attempt['error_message']!r} — but the heartbeat arm selected this "
        "row precisely because its lock_expires_at was still an hour in the "
        "future. The audit trail must not assert a lock expiry that never "
        "happened; name the heartbeat deadline (the event's cause key "
        "already carries it)."
    )


# ── in-memory twin: parity pins and one parity red ───────────────────


async def test_twin_deadline_boundary_is_strictly_past() -> None:
    """The twin's deadline comparison must be strict (age exactly AT the
    timeout is not silence past it), and the arm split at the lease
    boundary must mirror the SQL's: lease exactly now belongs to the
    heartbeat arm (its ``>=``), lease any amount past belongs to the
    lease arm. The FakeClock makes "exactly" testable deterministically.
    """
    backend = _twin_backend()
    exactly_at = _twin_running_row(
        backend,
        heartbeat_at=_TWIN_START - timedelta(seconds=30),
        heartbeat_timeout=timedelta(seconds=30),
        lease=_TWIN_START + timedelta(hours=1),
    )
    one_us_past = _twin_running_row(
        backend,
        heartbeat_at=_TWIN_START - timedelta(seconds=30, microseconds=1),
        heartbeat_timeout=timedelta(seconds=30),
        lease=_TWIN_START + timedelta(hours=1),
    )
    lease_exactly_now = _twin_running_row(
        backend,
        heartbeat_at=_TWIN_START - timedelta(hours=1),
        heartbeat_timeout=timedelta(seconds=30),
        lease=_TWIN_START,
    )
    lease_one_us_past = _twin_running_row(
        backend,
        heartbeat_at=_TWIN_START - timedelta(hours=1),
        heartbeat_timeout=timedelta(seconds=30),
        lease=_TWIN_START - timedelta(microseconds=1),
    )

    count = await backend.reclaim_expired_locks(_GRACE, _GRACE)

    assert count == 3, (
        f"exactly the three rows past their arm's deadline are reclaimable, got {count}"
    )
    exactly_at_row = await backend.get(exactly_at)
    assert exactly_at_row is not None and exactly_at_row.status == "running", (
        "a holder silent EXACTLY as long as its heartbeat_timeout was "
        "reclaimed — the deadline comparison must be strict (silence must "
        "run PAST the timeout), matching the SQL's <."
    )
    one_us_row = await backend.get(one_us_past)
    assert one_us_row is not None and one_us_row.status != "running"
    lease_now_detail = await _twin_reclaim_detail(backend, lease_exactly_now)
    assert lease_now_detail.get("cause") == "heartbeat_timeout", (
        f"lease exactly at now is still-valid (>=), so the heartbeat arm "
        f"owns the row (detail={lease_now_detail!r})"
    )
    lease_past_detail = await _twin_reclaim_detail(backend, lease_one_us_past)
    assert lease_past_detail.get("cause") == "lock_expired", (
        f"lease any amount past is the lease arm's row (detail={lease_past_detail!r})"
    )


async def test_twin_null_knob_and_unstamped_beat_mirror_the_sql() -> None:
    """Twin parity for the never-match pin: no knob (however ancient the
    beat) and no stamped beat (however tight the knob) both wait for
    their lease, exactly as the SQL does."""
    backend = _twin_backend()
    no_knob = _twin_running_row(
        backend,
        heartbeat_at=_TWIN_START - timedelta(hours=1),
        heartbeat_timeout=None,
        lease=_TWIN_START + timedelta(hours=1),
    )
    no_beat = _twin_running_row(
        backend,
        heartbeat_at=None,
        heartbeat_timeout=timedelta(seconds=30),
        lease=_TWIN_START + timedelta(hours=1),
    )
    control = _twin_running_row(
        backend,
        heartbeat_at=_TWIN_START - timedelta(hours=1),
        heartbeat_timeout=timedelta(seconds=30),
        lease=_TWIN_START + timedelta(hours=1),
    )

    count = await backend.reclaim_expired_locks(_GRACE, _GRACE)

    assert count == 1, "only the control row is reclaimable"
    control_row = await backend.get(control)
    assert control_row is not None and control_row.status != "running", (
        "the twin's heartbeat arm did not run (the control row was not "
        "reclaimed), so the two never-match assertions below prove nothing."
    )
    for job_id in (no_knob, no_beat):
        row = await backend.get(job_id)
        assert row is not None and row.status == "running", (
            "the twin reclaimed a NULL-knob or unstamped-beat row whose "
            "lease was valid — the SQL waits for the lease in both shapes."
        )


async def test_twin_both_arms_row_is_lease_owned_and_written_once() -> None:
    """Twin parity for the disjointness pin: a both-eligible row is the
    lease arm's, with exactly one attempt row written."""
    backend = _twin_backend()
    both = _twin_running_row(
        backend,
        heartbeat_at=_TWIN_START - timedelta(hours=1),
        heartbeat_timeout=timedelta(seconds=30),
        lease=_TWIN_START - timedelta(seconds=10),
    )

    count = await backend.reclaim_expired_locks(_GRACE, _GRACE)

    assert count == 1
    detail = await _twin_reclaim_detail(backend, both)
    assert detail.get("cause") == "lock_expired", (
        f"the twin's if/elif must order the lease arm first so a "
        f"both-eligible row is lease-owned, like the SQL's disjoint arms "
        f"(detail={detail!r})"
    )
    attempts = backend._attempts.get(both, [])
    assert len(attempts) == 1, (
        f"a both-eligible row must carry exactly one attempt row, got {len(attempts)}"
    )


async def test_twin_cancel_carve_out_and_branch_labels_mirror_the_sql() -> None:
    """Twin parity for the carve-out pin: the grace ladder applies to the
    heartbeat deadline, both cancel-in-flight rows (exhausted AND
    retryable) land 'cancelled' with the cancel columns preserved
    (operator intent outranks the retry budget (#238)), mirroring the
    reordered CASE, and the no-cancel exhausted row stays the crashed
    arm."""
    backend = _twin_backend()
    timeout = timedelta(seconds=30)
    deadline_past_inside = timedelta(seconds=_CANCEL_MARGIN_SECONDS - 20)
    deadline_past_deep = timedelta(seconds=_CANCEL_MARGIN_SECONDS + 110)
    inside_margin = _twin_running_row(
        backend,
        heartbeat_at=_TWIN_START - (timeout + deadline_past_inside),
        heartbeat_timeout=timeout,
        lease=_TWIN_START + timedelta(hours=1),
        cancel_phase=1,
    )
    deep_exhausted = _twin_running_row(
        backend,
        heartbeat_at=_TWIN_START - (timeout + deadline_past_deep),
        heartbeat_timeout=timeout,
        lease=_TWIN_START + timedelta(hours=1),
        cancel_phase=1,
        max_attempts=1,
        attempt=1,
    )
    deep_retryable = _twin_running_row(
        backend,
        heartbeat_at=_TWIN_START - (timeout + deadline_past_deep),
        heartbeat_timeout=timeout,
        lease=_TWIN_START + timedelta(hours=1),
        cancel_phase=1,
        max_attempts=3,
        attempt=1,
    )

    count = await backend.reclaim_expired_locks(_GRACE, _GRACE)

    assert count == 2
    inside_row = await backend.get(inside_margin)
    assert inside_row is not None and inside_row.status == "running", (
        "the twin reclaimed a cancel-in-flight row inside the carve-out "
        "margin — the twin's margin must ride the HEARTBEAT deadline too."
    )
    exhausted_row = await backend.get(deep_exhausted)
    assert exhausted_row is not None and exhausted_row.status == "cancelled"
    retry_row = await backend.get(deep_retryable)
    assert retry_row is not None and retry_row.status == "cancelled", (
        "the twin's cancel arm must outrank the budget arm, exactly as "
        "the reordered _SWEEP_1_SQL CASE does: the old budget-first "
        "twin re-pended this row and wiped the operator's cancel"
    )
    assert retry_row.cancel_phase == CancelPhase.COOPERATIVE, (
        "the twin's cancel arm preserves cancel_phase as the audit trail"
    )
    assert retry_row.cancel_requested_at is not None, (
        "the twin's cancel arm preserves cancel_requested_at as the audit trail"
    )
    detail = await _twin_reclaim_detail(backend, deep_exhausted)
    assert detail.get("cause") == "heartbeat_timeout", (
        f"the twin's carve-out path is still the heartbeat arm's reclaim (detail={detail!r})"
    )


async def test_twin_batch_caps_are_per_arm() -> None:
    """Twin parity for the per-arm LIMIT: batch_size=1 with one row
    eligible per arm reclaims both in one call."""
    backend = _twin_backend()
    _twin_running_row(
        backend,
        heartbeat_at=_TWIN_START - timedelta(hours=1),
        heartbeat_timeout=timedelta(seconds=30),
        lease=_TWIN_START + timedelta(hours=1),
    )
    _twin_running_row(
        backend,
        heartbeat_at=_TWIN_START,
        heartbeat_timeout=timedelta(seconds=30),
        lease=_TWIN_START - timedelta(seconds=10),
    )

    count = await backend.reclaim_expired_locks(_GRACE, _GRACE, batch_size=1)

    assert count == 2, (
        f"the twin's per-arm caps must mirror the SQL's independent LIMITs: "
        f"one row per arm at batch_size=1 reclaims both, got {count}"
    )


async def test_twin_mirrors_the_sqls_past_beat_conjunct() -> None:
    """RED at commit time (expected, the deliverable): the twin's
    heartbeat arm (``src/taskq/testing/_sweeps.py``) omits the SQL's
    ``last_heartbeat_at < statement_timestamp()`` conjunct — for
    positive timeouts the deadline arithmetic implies it, but for the
    direct-SQL-reachable degenerate row (a FUTURE-stamped beat plus a
    negative timeout) the deadline alone admits a row the SQL provably
    never reclaims (pinned green one section above, against PG).

    The twin's own docstring claims it mirrors ``_SWEEP_1_SQL``
    "exactly, in both directions", and it DOES guard the sibling
    NULL-beat state — the future-beat state is the same direct-SQL
    class, one conjunct away. Severity is parity/defense-in-depth: the
    state is unreachable through the public API (enqueue refuses
    non-positive timeouts; dispatch stamps the beat), so no production
    behaviour diverges — only the twin's seam contract does.
    """
    backend = _twin_backend()
    future_beat = _twin_running_row(
        backend,
        # Beat stamped 10s into the future, timeout -30s: the deadline
        # arithmetic reads "20s past" while the beat is not in the past.
        heartbeat_at=_TWIN_START + timedelta(seconds=10),
        heartbeat_timeout=timedelta(seconds=-30),
        lease=_TWIN_START + timedelta(hours=1),
    )
    control = _twin_running_row(
        backend,
        heartbeat_at=_TWIN_START - timedelta(hours=1),
        heartbeat_timeout=timedelta(seconds=30),
        lease=_TWIN_START + timedelta(hours=1),
    )

    await backend.reclaim_expired_locks(_GRACE, _GRACE)

    control_row = await backend.get(control)
    assert control_row is not None and control_row.status != "running", (
        "the twin's heartbeat arm must have run (the control row is reclaimed)"
    )
    future_row = await backend.get(future_beat)
    assert future_row is not None, "the twin must read back the seeded row"
    assert future_row.status == "running", (
        f"the twin reclaimed a row whose last_heartbeat_at is stamped in the "
        f"FUTURE (status={future_row.status!r}) — the SQL's heartbeat arm "
        "requires last_heartbeat_at < statement_timestamp() and leaves this "
        "row running (pinned against PG above), so the twin's deadline-only "
        "predicate has drifted from the SQL it claims to mirror exactly."
    )


# ── docs RED: the ops footgun registry's missing row ─────────────────


def test_ops_footgun_registry_names_the_inversion_trap() -> None:
    """The ops guide's footgun registry must name the lease-inversion trap.

    A ``heartbeat_timeout`` LARGER than the lock lease is meaningless —
    the lease reclaims first — and under the enforcement direction that
    trap is live: the knob
    silently no-ops for the entire at-or-above-the-lease range (pinned
    behaving exactly so one section above), the enqueue boundary cannot
    see the fleet's lease (``build_enqueue_args`` is deliberately pure),
    and no layer that CAN see both values (dispatch stamps the lease and
    reads the knob) warns on the inversion.

    ``docs/guides/ops.md``'s footgun registry is the project's
    misconfiguration registry and already carries the SIBLING trap (the
    lower-bound row: "heartbeat_timeout set below one heartbeat
    interval"); docs-contract pins are house style
    (tests/test_max_concurrent_docs_contract.py). The inversion row is
    one table line: name the knob, name the lease, say the knob never
    governs. A runtime warning at dispatch is the stronger fix (the
    ``actors-on-unconsumed-queues`` boot-warning precedent); this pin
    accepts either and demands at least the registry line.
    """
    text = _OPS_MD.read_text()
    collapsed = " ".join(text.split())

    # Locator: the sibling lower-bound row must still be registered — if
    # it moved, this pin's registry moved with it, and the locator should
    # be updated in the same change.
    assert "heartbeat_timeout" in collapsed and "below one heartbeat interval" in collapsed, (
        "the ops footgun table's heartbeat_timeout lower-bound row moved or "
        "was renamed — update this pin's locator alongside the registry."
    )

    lease_tokens = ("TASKQ_LOCK_LEASE", "lock_lease")
    # Phrases that can only appear in an inversion statement — "governs"
    # alone is deliberately excluded: the feature bullet already says "
    # the shorter of the two deadlines governs", which states the rule "
    # without naming the trap the rule creates.
    inversion_tokens = (
        "never",
        "inert",
        "meaningless",
        "no-op",
        "silently",
        "reclaims first",
    )

    def _inversion_named() -> bool:
        start = 0
        while True:
            idx = collapsed.find("heartbeat_timeout", start)
            if idx == -1:
                return False
            window = collapsed[max(0, idx - 200) : idx + 400]
            if any(token in window for token in lease_tokens) and any(
                token in window for token in inversion_tokens
            ):
                return True
            start = idx + 1

    assert _inversion_named(), (
        "docs/guides/ops.md's footgun registry names the lower-bound "
        "heartbeat_timeout trap (a value below one heartbeat interval "
        "reclaims a healthy job on a missed beat) but not the inversion: a "
        "heartbeat_timeout at or above the fleet's TASKQ_LOCK_LEASE never "
        "governs — the lease deadline always fires first, the per-job "
        "budget silently no-ops, and nothing at enqueue, dispatch, or sweep "
        "says so. That is the inversion class: a safety knob that "
        "silently does nothing. Register the trap (one footgun-table row: "
        "name the knob, name TASKQ_LOCK_LEASE, say the knob never governs / "
        "the lease reclaims first), or warn at dispatch where both values "
        "are known — either satisfies this pin."
    )


# ── non-positive stored heartbeat_timeout must never govern ──────────


@pytest.mark.integration
async def test_non_positive_stored_heartbeat_timeout_never_reclaims_a_healthy_holder(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """A stored ``heartbeat_timeout`` of zero or a negative interval must
    be inert: the heartbeat arm must leave such a row running and let its
    lease govern, exactly as it does for a row that carries no knob at
    all.

    Enqueue validation refuses a non-positive ``heartbeat_timeout``, but
    that is the only gate in the system. A row can carry ``0`` or a
    negative interval by any path that writes the column directly — rows
    stored before the knob was validated and enforced, a manual UPDATE,
    any future write path — and the column carries no CHECK constraint.
    With such a value ``last_heartbeat_at + heartbeat_timeout <
    statement_timestamp()`` is true from the instant the row is written,
    so a maximally healthy holder, beating right now with an hour of
    lease left, is reclaimed as a false crash on the very first sweep.

    Operationally this is worse than the documented lower-bound sizing
    trap: that trap at least requires a genuinely missed beat. A
    degenerate stored value needs none — the row is eligible before the
    holder could possibly have missed anything, so a rolling upgrade
    silently discards in-flight work fleet-wide.
    """
    schema = module_pg_schema.schema_name
    worker_id = new_uuid()
    await create_worker(clean_pg_conn, schema, worker_id)

    # Healthy, actively-heartbeating job — degenerate zero timeout only.
    zero_timeout = await _seed_hb_running_job(
        clean_pg_conn,
        schema,
        worker_id,
        heartbeat_age=timedelta(0),
        heartbeat_timeout=timedelta(0),
        lease_expires_in=timedelta(hours=1),
    )
    # Same shape with a negative stored timeout (also unreachable via
    # enqueue, also unguarded by any schema constraint or sweep check).
    negative_timeout = await _seed_hb_running_job(
        clean_pg_conn,
        schema,
        worker_id,
        heartbeat_age=timedelta(0),
        heartbeat_timeout=timedelta(seconds=-5),
        lease_expires_in=timedelta(hours=1),
    )
    # Control: a real, positive, well-sized timeout with the same fresh
    # beat must NOT be reclaimed.
    control = await _seed_hb_running_job(
        clean_pg_conn,
        schema,
        worker_id,
        heartbeat_age=timedelta(0),
        heartbeat_timeout=timedelta(seconds=30),
        lease_expires_in=timedelta(hours=1),
    )

    count = await PostgresBackend.sweep_expired_locks(clean_pg_conn, _GRACE, _GRACE, schema=schema)

    assert await _job_status(clean_pg_conn, schema, control) == "running", (
        "control row (positive, well-sized heartbeat_timeout, fresh beat) "
        "was reclaimed — seeding or sweep call is broken, not the bug under test."
    )
    assert await _job_status(clean_pg_conn, schema, zero_timeout) == "running", (
        f"a job with heartbeat_timeout=0 and a beat stamped at seed time (no "
        f"missed heartbeat whatsoever, lease valid for another hour) was "
        f"reclaimed by the sweep (count={count}). The heartbeat arm guards "
        f"only `heartbeat_timeout IS NOT NULL`, with no positivity conjunct, "
        f"and the column carries no CHECK constraint — so a non-positive "
        f"stored value reclaims a perfectly healthy running job as a false "
        f"crash on the very first sweep after dispatch."
    )
    assert await _job_status(clean_pg_conn, schema, negative_timeout) == "running", (
        f"a healthy, actively-heartbeating job with heartbeat_timeout=-5s was "
        f"reclaimed by the sweep (count={count}) with zero missed beats — the "
        f"heartbeat arm must treat a non-positive stored timeout as inert and "
        f"let the lease govern."
    )


@pytest.mark.integration
async def test_heartbeat_timeout_at_the_documented_sizing_leaves_a_beating_holder_alone(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """A job sized exactly as the ops guide instructs — ``heartbeat_timeout``
    at twice the fleet's heartbeat interval and comfortably below the lock
    lease — must survive every sweep while its holder keeps beating.

    This is the sizing an operator who follows the documentation actually
    deploys, so it is the shape that must never produce a false reclaim.
    The row is swept twice with a beat refreshed in between, which is what
    a live holder does: a reclaim here would mean the enforcement arm eats
    healthy work at the only sizing the guide blesses.
    """
    schema = module_pg_schema.schema_name
    worker_id = new_uuid()
    await create_worker(clean_pg_conn, schema, worker_id)
    # Documented window: >= 2x heartbeat_interval, < lock_lease.
    heartbeat_interval = timedelta(seconds=5)
    lock_lease = timedelta(seconds=60)
    timeout = 2 * heartbeat_interval
    assert timeout < lock_lease, "the fixture must sit inside the documented window"

    job_id = await _seed_hb_running_job(
        clean_pg_conn,
        schema,
        worker_id,
        # One interval of silence: a holder beating on schedule.
        heartbeat_age=heartbeat_interval,
        heartbeat_timeout=timeout,
        lease_expires_in=lock_lease,
    )

    for _ in range(2):
        count = await PostgresBackend.sweep_expired_locks(
            clean_pg_conn, _GRACE, _GRACE, schema=schema
        )
        assert count == 0, (
            f"the sweep reclaimed {count} rows while the only running job was "
            "beating on schedule at the documented sizing (heartbeat_timeout = "
            "2x heartbeat_interval, below lock_lease) — heartbeat enforcement "
            "must never touch a healthy holder."
        )
        assert await _job_status(clean_pg_conn, schema, job_id) == "running"
        # The holder's next beat lands.
        await clean_pg_conn.execute(
            f'UPDATE "{schema}".jobs SET last_heartbeat_at = clock_timestamp() WHERE id = $1',
            job_id,
        )

    assert await _attempt_count(clean_pg_conn, schema, job_id) == 0, (
        "a healthy holder must accrue no crash attempt rows"
    )
    assert await _event_count(clean_pg_conn, schema, job_id) == 0, (
        "a healthy holder must accrue no reclaim events"
    )


async def test_twin_treats_a_non_positive_stored_heartbeat_timeout_as_inert() -> None:
    """Backend parity for the non-positive stored timeout: the in-memory
    twin must leave a zero or negative ``heartbeat_timeout`` row running
    on a fresh beat, exactly as the SQL arm must.

    The twin is the backend every consumer test runs against, so a twin
    that reclaims these rows hides the production defect from the whole
    suite — and a twin that keeps reclaiming them after the SQL is fixed
    breaks the seam-equivalence contract from the other direction.
    """
    backend = _twin_backend()
    zero_timeout = _twin_running_row(
        backend,
        heartbeat_at=_TWIN_START,
        heartbeat_timeout=timedelta(0),
        lease=_TWIN_START + timedelta(hours=1),
    )
    negative_timeout = _twin_running_row(
        backend,
        heartbeat_at=_TWIN_START,
        heartbeat_timeout=timedelta(seconds=-5),
        lease=_TWIN_START + timedelta(hours=1),
    )
    control = _twin_running_row(
        backend,
        heartbeat_at=_TWIN_START - timedelta(hours=1),
        heartbeat_timeout=timedelta(seconds=30),
        lease=_TWIN_START + timedelta(hours=1),
    )

    await backend.reclaim_expired_locks(_GRACE, _GRACE)

    control_row = await backend.get(control)
    assert control_row is not None and control_row.status != "running", (
        "the twin's heartbeat arm did not run (the control row is still "
        "running), so the assertions below prove nothing."
    )
    for job_id, label in ((zero_timeout, "zero"), (negative_timeout, "negative")):
        row = await backend.get(job_id)
        assert row is not None and row.status == "running", (
            f"the twin reclaimed a healthy, freshly-beating job carrying a "
            f"{label} stored heartbeat_timeout — a non-positive timeout must be "
            "inert on both backends, leaving the lease to govern."
        )


async def test_twin_heartbeat_attempt_row_does_not_claim_the_lock_expired() -> None:
    """Backend parity for the reclaim audit trail: the twin's attempt row
    for a heartbeat reclaim must name the heartbeat deadline, never a lock
    expiry.

    The heartbeat arm selects a row precisely because its lease is still
    valid, so an attempt row asserting "lock expired" is a falsehood an
    operator reconciling ``job_attempts`` against ``jobs`` cannot resolve.
    Both backends write this audit surface, so both must tell the truth.
    """
    backend = _twin_backend()
    job_id = _twin_running_row(
        backend,
        heartbeat_at=_TWIN_START - timedelta(hours=1),
        heartbeat_timeout=timedelta(seconds=30),
        lease=_TWIN_START + timedelta(hours=1),
        max_attempts=1,
        attempt=1,
    )

    count = await backend.reclaim_expired_locks(_GRACE, _GRACE)
    assert count == 1

    attempts = backend._attempts.get(job_id, [])
    assert len(attempts) == 1, f"the heartbeat reclaim must write one attempt row, got {attempts!r}"
    attempt = attempts[-1]
    assert attempt.outcome == "crashed"
    assert "lock expired" not in str(attempt.error_message), (
        f"the twin's attempt row for a HEARTBEAT reclaim says "
        f"{attempt.error_message!r}, but the arm selected this row with its "
        "lease an hour in the future — the audit trail must name the "
        "heartbeat deadline that actually fired."
    )

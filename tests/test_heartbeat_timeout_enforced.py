# ruff: noqa: S608  # Why: schema is fixture-derived (module_pg_schema), not user input; every value is $-bound.

"""Pins for ``heartbeat_timeout`` — a public safety parameter that was
accepted, stored, and read by nothing.

``heartbeat_timeout`` is plumbed end to end: accepted on the public API
(``client/_jobs.py``, ``client/_taskq.py``, ``client/_enqueuer.py``),
carried on ``EnqueueArgs``/``JobRow``, written to PG, hydrated back — and
was read by ZERO consumers: no validation, no warning, the call accepted
and discarded. A ``timedelta`` safety knob that silently does nothing is
a placeholder that returns a plausible value — the shape the
constitution's deferred-work rule exists to forbid.

The settled contract (#117, enforcement direction): ``heartbeat_timeout``
is ENFORCED. The enqueue boundary accepts a positive value (a non-positive
one is refused with a boundary error mirroring ``start_to_close``'s), and
the reclaim sweep's stale-holder classification reads it — a running job
whose holder has been silent past the job's ``heartbeat_timeout`` is
reclaimed exactly as an expired lock is, while its lock lease is still
valid (the lease is the per-worker global; the heartbeat timeout is the
per-job promise, and the shorter of the two governs). The
``job_events``/``job_attempts`` rows land through the same crash-reclaim
outbox channel (``reason='lock_expired'`` — the slice
``poll_reclaim_events`` tails and the retention carve-out keeps — with a
``cause`` key naming which deadline fired).
"""

import json
from dataclasses import replace as dataclass_replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

import pytest
from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq.backend.postgres import PostgresBackend
from taskq.testing.clock import FakeClock
from taskq.testing.fixtures import JobsApp, ModulePgSchema
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.jobs import make_job_row
from taskq.testing.pg import create_running_job, create_worker

_SRC = Path(__file__).resolve().parents[1] / "src" / "taskq"

_GRACE = timedelta(seconds=0)

#: The in-memory twin's deterministic clock start (same convention as
#: tests/test_in_memory_backend.py's ``_START``).
_TWIN_START = datetime(2025, 1, 1, tzinfo=UTC)


def _heartbeat_timeout_refused_at_enqueue() -> bool:
    """True when the client boundary refuses ``heartbeat_timeout`` loudly."""
    from taskq.actor import actor as actor_decorator
    from taskq.client._args import build_enqueue_args

    class _Payload(BaseModel):
        x: int = 0

    @actor_decorator(name="heartbeat_timeout_probe")
    async def _probe(payload: _Payload) -> None:
        pass

    try:
        build_enqueue_args(_probe, _Payload(), heartbeat_timeout=timedelta(seconds=30))
    except (ValueError, TypeError) as exc:
        if "heartbeat" in str(exc).lower():
            return True
        raise
    return False


def _enforcement_references() -> list[str]:
    """Files under worker/ or the sweep module that read ``heartbeat_timeout``.

    Storage/hydration (``backend/_records.py``) does not count — only a
    consumer that ACTS on the value is enforcement.
    """
    hits: list[str] = []
    candidates = [*(_SRC / "worker").glob("**/*.py"), _SRC / "backend" / "_sweeps.py"]
    for path in candidates:
        for lineno, line in enumerate(path.read_text().splitlines(), start=1):
            if "heartbeat_timeout" in line:
                hits.append(f"{path.relative_to(_SRC)}:{lineno}")
    return hits


def test_heartbeat_timeout_is_enforced_or_refused() -> None:
    """The parameter must not be silently inert: either the client refuses
    it (mirroring the priority smallint guard) or something in the worker
    / sweep path reads it to enforce it."""
    refused = _heartbeat_timeout_refused_at_enqueue()
    enforced_at = _enforcement_references()
    assert refused or enforced_at, (
        "heartbeat_timeout is accepted by build_enqueue_args, stored on the "
        "job row, and read by NOTHING under src/taskq/worker/ or the sweep "
        "module (verified: zero references). A job that stops heartbeating "
        "is reclaimed only when its global lock_lease expires — the per-job "
        "timeout the caller asked for is silently discarded. Enforce it (a "
        "per-job disjunct in the reclaim sweep against last_heartbeat_at) "
        "or refuse it at enqueue; a documented no-op is neither."
    )


def test_heartbeat_timeout_is_accepted_and_validated_at_enqueue() -> None:
    """The enforcement direction's client half: a positive
    ``heartbeat_timeout`` is carried onto ``EnqueueArgs`` (the column the
    sweep reads), and a non-positive one is refused with a boundary error
    mirroring ``start_to_close``'s — a zero-or-negative timeout would
    anchor the staleness deadline in the past and reclaim a healthy job
    on the first sweep tick."""
    from taskq.actor import actor as actor_decorator
    from taskq.client._args import build_enqueue_args

    class _Payload(BaseModel):
        x: int = 0

    @actor_decorator(name="heartbeat_timeout_accept_probe")
    async def _accept_probe(payload: _Payload) -> None:
        pass

    args = build_enqueue_args(_accept_probe, _Payload(), heartbeat_timeout=timedelta(seconds=30))
    assert args.heartbeat_timeout == timedelta(seconds=30), (
        f"build_enqueue_args dropped heartbeat_timeout (got {args.heartbeat_timeout!r}); "
        "the value never reaches the jobs row the reclaim sweep reads."
    )

    for bad in (timedelta(0), timedelta(seconds=-1)):
        with pytest.raises(ValueError, match="heartbeat_timeout") as excinfo:
            build_enqueue_args(_accept_probe, _Payload(), heartbeat_timeout=bad)
        assert "must be > 0" in str(excinfo.value)


@pytest.mark.integration
async def test_stale_heartbeat_reclaims_running_job(
    module_pg_schema: ModulePgSchema,
    clean_jobs_app: JobsApp,
) -> None:
    """The enforcement direction's sweep half. Three holders, one sweep:

    * **stale holder** — heartbeat 1 h old past a 30 s ``heartbeat_timeout``
      while the lock lease is still valid for another hour. Only the
      heartbeat arm can reclaim this row; the lease arm's predicate is
      false by an hour.
    * **fresh holder** — same ``heartbeat_timeout``, heartbeat stamped
      now. Its leased job must NOT be reclaimed: the sweep classifies by
      the holder's silence, not by the mere presence of the knob.
    * **fresh heartbeat, expired lease** — the lease arm must still
      reclaim a heartbeat-configured row (the new arm narrows nothing the
      lease arm owned; it only adds the earlier, per-job deadline).

    The stale holder's reclaim event rides the existing crash-reclaim
    outbox channel (``reason='lock_expired'``) with ``cause`` naming the
    heartbeat deadline.
    """
    schema = module_pg_schema.schema_name
    deps = clean_jobs_app.deps
    worker_id = new_uuid()
    async with deps.worker_pool.acquire() as conn:
        await create_worker(conn, schema, worker_id)
        # Stale holder: only the heartbeat arm can reclaim (lease +1 h).
        job_stale = await create_running_job(
            conn,
            schema,
            worker_id,
            lock_expires_at=datetime.now(UTC) + timedelta(hours=1),
            with_events=False,
        )
        await conn.execute(
            f'UPDATE "{schema}".jobs '
            "SET heartbeat_timeout = interval '30 seconds', "
            "    last_heartbeat_at = clock_timestamp() - interval '1 hour' "
            "WHERE id = $1",
            job_stale,
        )
        # Fresh holder: same knob, heartbeat stamped at insert time.
        job_fresh = await create_running_job(
            conn,
            schema,
            worker_id,
            lock_expires_at=datetime.now(UTC) + timedelta(hours=1),
            with_events=False,
        )
        await conn.execute(
            f"UPDATE \"{schema}\".jobs SET heartbeat_timeout = interval '30 seconds' WHERE id = $1",
            job_fresh,
        )
        # Fresh heartbeat, expired lease: the lease arm owns this row.
        job_lease = await create_running_job(
            conn,
            schema,
            worker_id,
            lock_expires_at=datetime.now(UTC) - timedelta(seconds=10),
            with_events=False,
        )
        await conn.execute(
            f"UPDATE \"{schema}\".jobs SET heartbeat_timeout = interval '30 seconds' WHERE id = $1",
            job_lease,
        )

        reclaimed = await PostgresBackend.sweep_expired_locks(conn, _GRACE, _GRACE, schema=schema)

        statuses: dict[str, str] = {}
        for label, job_id in (("stale", job_stale), ("fresh", job_fresh), ("lease", job_lease)):
            statuses[label] = await conn.fetchval(  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]  # Why: asyncpg fetchval returns Any on a $-bound one-column select; narrowed by the dict[str, str] assignment.
                f'SELECT status::text FROM "{schema}".jobs WHERE id = $1', job_id
            )
        stale_detail_raw = await conn.fetchval(  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]  # Why: same as above.
            f'SELECT detail::text FROM "{schema}".job_events '
            "WHERE job_id = $1 AND kind = 'state_change' "
            "ORDER BY id DESC LIMIT 1",
            job_stale,
        )

    assert reclaimed == 2, (
        f"sweep reclaimed {reclaimed} rows; expected exactly the stale-holder "
        f"and expired-lease rows (statuses={statuses!r})."
    )
    assert statuses["stale"] != "running", (
        f"a running job with heartbeat_timeout=30s and a heartbeat 1h stale "
        f"survived the reclaim sweep untouched (status={statuses['stale']!r}, "
        "lock lease still valid). The sweep reclaims on the global lock_lease "
        "only; nothing reads the per-job heartbeat_timeout the row carries."
    )
    assert statuses["fresh"] == "running", (
        f"a FRESH holder's leased job was reclaimed (status={statuses['fresh']!r}): "
        "the heartbeat arm must classify by the holder's silence, not by the "
        "mere presence of heartbeat_timeout on the row."
    )
    assert statuses["lease"] != "running", (
        f"the lease arm stopped reclaiming an expired lease on a "
        f"heartbeat-configured row (status={statuses['lease']!r}): the "
        "heartbeat arm must add an earlier deadline, never narrow the lease arm."
    )

    stale_detail: dict[str, object] = json.loads(str(stale_detail_raw))
    assert stale_detail.get("reason") == "lock_expired", (
        f"the heartbeat reclaim must ride the crash-reclaim outbox channel "
        f"poll_reclaim_events tails and the retention carve-out keeps "
        f"(detail={stale_detail!r})."
    )
    assert stale_detail.get("cause") == "heartbeat_timeout", (
        f"the reclaim event must name which deadline fired (detail={stale_detail!r})."
    )


async def test_in_memory_reclaim_enforces_heartbeat_timeout() -> None:
    """The in-memory twin mirrors the sweep's heartbeat arm in both
    directions: a holder silent past the job's ``heartbeat_timeout`` is
    reclaimed while its lease is still valid; a fresh holder's leased job
    is not; and the lease arm still reclaims a heartbeat-configured row
    whose lease expired — the same three-way contract the PG pin above
    asserts, so the twin cannot drift from the SQL."""
    backend = InMemoryBackend(
        clock=FakeClock(_TWIN_START),
        cancellation_grace_period=_GRACE,
        cleanup_grace_period=_GRACE,
    )

    def _running_row(
        *, heartbeat_at: datetime, heartbeat_timeout: timedelta, lease: datetime
    ) -> UUID:
        row = make_job_row(heartbeat_timeout=heartbeat_timeout)
        running = dataclass_replace(
            row,
            status="running",
            locked_by_worker=backend._worker_id,  # pyright: ignore[reportPrivateUsage]  # Why: the twin's private store is the family's test seam (tests/test_in_memory_backend.py uses the same).
            lock_expires_at=lease,
            last_heartbeat_at=heartbeat_at,
        )
        backend._jobs[running.id] = running  # pyright: ignore[reportPrivateUsage]  # Why: same seam as above.
        return running.id

    job_stale = _running_row(
        heartbeat_at=_TWIN_START - timedelta(hours=1),
        heartbeat_timeout=timedelta(seconds=30),
        lease=_TWIN_START + timedelta(hours=1),
    )
    job_fresh = _running_row(
        heartbeat_at=_TWIN_START - timedelta(seconds=5),
        heartbeat_timeout=timedelta(seconds=30),
        lease=_TWIN_START + timedelta(hours=1),
    )
    job_lease = _running_row(
        heartbeat_at=_TWIN_START - timedelta(seconds=5),
        heartbeat_timeout=timedelta(seconds=30),
        lease=_TWIN_START - timedelta(seconds=10),
    )

    count = await backend.reclaim_expired_locks(_GRACE, _GRACE)

    stale = await backend.get(job_stale)
    fresh = await backend.get(job_fresh)
    lease = await backend.get(job_lease)
    assert stale is not None and fresh is not None and lease is not None
    assert count == 2, "only the stale-holder and expired-lease rows are reclaimable"
    assert stale.status != "running", (
        f"holder silent 1h past a 30s heartbeat_timeout survived the twin's "
        f"reclaim (status={stale.status!r}) while its lease was still valid."
    )
    assert fresh.status == "running", (
        f"a fresh holder's leased job was reclaimed by the twin (status={fresh.status!r})."
    )
    assert lease.status != "running", (
        f"the twin's lease arm stopped reclaiming an expired lease on a "
        f"heartbeat-configured row (status={lease.status!r})."
    )

    stale_events = [e for e in await backend.get_events(job_stale) if e.kind == "state_change"]
    assert stale_events, "the twin wrote no state_change event for the reclaim"
    detail = stale_events[-1].detail
    assert detail.get("reason") == "lock_expired"
    assert detail.get("cause") == "heartbeat_timeout", (
        f"the twin's reclaim event must name the deadline that fired "
        f"(detail={detail!r}) — the PG sweep's event does."
    )

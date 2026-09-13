# ruff: noqa: S608  # Why: schema is fixture-derived (module_pg_schema), not user input; every value is $-bound.

"""Red-team pins for ``heartbeat_timeout`` — a public safety parameter that
is accepted, stored, and read by nothing.

``heartbeat_timeout`` is plumbed end to end: accepted on the public API
(``client/_jobs.py``, ``client/_taskq.py``, ``client/_enqueuer.py``),
carried on ``EnqueueArgs``/``JobRow``, written to PG, hydrated back — and
appears ZERO times under ``src/taskq/worker/`` or in any sweep. There is
no validation and no warning: the call is accepted and discarded. A
``timedelta`` safety knob that silently does nothing is a placeholder
that returns a plausible value — the shape the constitution's
deferred-work rule exists to forbid.

The settled contract: ``heartbeat_timeout`` is ENFORCED or REFUSED, never
silently inert. Enforcement is the reclaim sweep's existing job with one
added disjunct — a running job whose ``last_heartbeat_at`` is older than
its ``heartbeat_timeout`` is reclaimed exactly as an expired lock is.
Refusal is a validator mirroring the ``priority`` smallint guard at the
client boundary. Either closes the defect; silence does not.

Both tests below are green under either fix direction and red today.
"""

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq.backend.postgres import PostgresBackend
from taskq.testing.fixtures import JobsApp, ModulePgSchema
from taskq.testing.pg import create_running_job, create_worker

_SRC = Path(__file__).resolve().parents[1] / "src" / "taskq"

_GRACE = timedelta(seconds=0)


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


@pytest.mark.integration
async def test_stale_heartbeat_reclaims_running_job(
    module_pg_schema: ModulePgSchema,
    clean_jobs_app: JobsApp,
) -> None:
    """A running job whose heartbeat is older than its ``heartbeat_timeout``
    must be reclaimed by the sweep even while its lock lease is still
    valid — the lock lease is the per-worker global; the heartbeat timeout
    is the per-job promise."""
    if _heartbeat_timeout_refused_at_enqueue():
        # Refused at the boundary: no job can carry the value, so there is
        # nothing to enforce. The refusal arm above is the evidence; this
        # test's contract is vacuous under that fix direction.
        return

    schema = module_pg_schema.schema_name
    deps = clean_jobs_app.deps
    worker_id = new_uuid()
    async with deps.worker_pool.acquire() as conn:
        await create_worker(conn, schema, worker_id)
        job_id = await create_running_job(
            conn,
            schema,
            worker_id,
            # Lock lease far in the future: the existing lock-expiry
            # predicate must NOT fire — only a heartbeat_timeout disjunct
            # can reclaim this row.
            lock_expires_at=datetime.now(UTC) + timedelta(hours=1),
            with_events=False,
        )
        await conn.execute(
            f'UPDATE "{schema}".jobs '
            "SET heartbeat_timeout = interval '30 seconds', "
            "    last_heartbeat_at = clock_timestamp() - interval '1 hour' "
            "WHERE id = $1",
            job_id,
        )

        reclaimed = await PostgresBackend.sweep_expired_locks(conn, _GRACE, _GRACE, schema=schema)

        status: str = await conn.fetchval(
            f'SELECT status FROM "{schema}".jobs WHERE id = $1',
            job_id,
        )

    assert reclaimed > 0 and status != "running", (
        f"a running job with heartbeat_timeout=30s and a heartbeat 1h stale "
        f"survived the reclaim sweep untouched (reclaimed={reclaimed}, "
        f"status={status!r}, lock lease still valid). The sweep reclaims on "
        "the global lock_lease only; nothing reads the per-job "
        "heartbeat_timeout the row carries."
    )

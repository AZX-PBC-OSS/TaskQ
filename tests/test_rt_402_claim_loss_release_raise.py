"""Issue 402: a dispatch round that commits its claim and then raises.

Deterministic forced injection, no load lottery. The claim statement is
autocommit by design (``taskq.backend._dispatch._dispatch_batch``), so a
round that has claimed rows is already committed server-side before the
round's remaining steps run. The issue names the pool release as the one
await left in that window: asyncpg's ``PoolConnectionHolder.release``
re-raises a failed ``Connection.reset()`` (``finally: raise ex``), so a
socket that dies between the claim's commit and the release makes
``pool_ctx.__aexit__`` raise AFTER the rows are ``running`` and locked to
this worker. The producer's transient arm then continues knowing nothing
was claimed.

The injection raises exactly there: the real claim runs to its commit on
a real connection, and the round's ``__aexit__`` then raises
``ConnectionDoesNotExistError`` after releasing the connection, the
observable shape of a dead-socket reset. The assertions:

* the stranded shape exists (rows running, locked to this worker, lease
  actively renewed) immediately after the raise;
* recovery is bounded: the heartbeat's claim-loss reconcile disowns the
  orphan rows once their ``started_at`` passes one full lease, the lease
  lapses, Sweep 1 re-pends, and the jobs re-dispatch and finish. At
  d0930101 (the reconcile's introduction) this test is green; at the
  pre-reconcile code (33e02b0d, the SHA the issue measured) the same
  injection strands the rows for the worker's lifetime - the red.
"""

# ruff: noqa: S608  # Why: schema is a fixture identifier validated by the backend; every value is $-bound or a module constant.

from __future__ import annotations

import asyncio
import contextlib
from typing import Any
from urllib.parse import urlparse, urlunparse

import asyncpg
import pytest
from pydantic import BaseModel

from taskq.actor import actor
from taskq.context import JobContext
from taskq.settings import WorkerSettings
from taskq.testing.health import unique_health_sock_path
from taskq.worker.run import _main

pytestmark = [pytest.mark.integration]

_QUEUE = "issue402_q"
_TAG = "issue402"

#: The reconcile's bound, in settings terms: the probe's grace is one
#: full lease on started_at (5 s here), the disowned row's lease then
#: lapses within one more lease, the sweep re-pends within its interval,
#: and the re-claim plus run is sub-second. 30 s is 3x that sum; a row
#: still non-terminal at the cap is the lost-job red.
_RECOVERY_BOUND_SECS = 30.0

#: The stranded-shape observation window: long enough for the producer's
#: transient arm to have logged and moved on and a couple of heartbeat
#: ticks to have renewed the lease, short of the reconcile's 5 s grace.
_STRANDED_OBSERVE_SECS = 2.0


class Issue402Payload(BaseModel):
    marker: str


@actor(name="issue402_ok", queue=_QUEUE)
async def issue402_ok(payload: Issue402Payload, ctx: JobContext[Issue402Payload]) -> None:
    _ = payload, ctx


_REGISTRY = {"issue402_ok": issue402_ok}


class _InjectionState:
    """Tracks whether a claim round has landed its commit."""

    def __init__(self) -> None:
        self.claimed = False
        self.raised = False
        # The job ids the claim round actually returned: the canary that
        # ties the injected raise to THIS test's jobs, not to some other
        # row the dispatch CTE happened to return.
        self.claimed_ids: list[str] = []


class _ConnProxy:
    """Forwards to the real connection and flags the claim CTE's rows.

    The dispatch CTE is the only statement the round runs that carries
    ``RETURNING j.*``; nonempty rows from it mean the claim COMMIT landed
    server-side (autocommit)."""

    def __init__(self, inner: Any, state: _InjectionState) -> None:
        self._inner = inner
        self._state = state

    async def fetch(self, sql: str, *args: Any) -> list[Any]:
        rows = await self._inner.fetch(sql, *args)
        if rows and "RETURNING j.*" in sql:
            self._state.claimed = True
            for row in rows:
                self._state.claimed_ids.append(str(row["id"]))
        return rows

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


class _AcquireCtxProxy:
    """The pool acquire context: real connection in, injected release raise out.

    The raise models asyncpg's ``PoolConnectionHolder.release`` re-raising
    a failed ``Connection.reset()`` on a dead socket: the connection is
    returned to the pool first (the healthy socket's release succeeds
    here; in production the reset terminates the connection), then the
    release's exception propagates out of ``__aexit__`` and the claimed
    ``JobRow`` batch is discarded."""

    def __init__(self, inner: Any, state: _InjectionState) -> None:
        self._inner = inner
        self._state = state

    async def __aenter__(self) -> Any:
        conn = await self._inner.__aenter__()
        return _ConnProxy(conn, self._state)

    async def __aexit__(self, *exc: Any) -> Any:
        if self._state.claimed and not self._state.raised:
            self._state.raised = True
            await self._inner.__aexit__(None, None, None)
            raise asyncpg.exceptions.ConnectionDoesNotExistError(
                "issue 402 injection: pool release reset on a dead socket"
            )
        return await self._inner.__aexit__(*exc)


class _PoolProxy:
    """Proxies only ``acquire``; every other pool attribute forwards."""

    def __init__(self, inner: Any, state: _InjectionState) -> None:
        self._inner = inner
        self._state = state

    def acquire(self, *, timeout: float | None = None) -> Any:
        return _AcquireCtxProxy(self._inner.acquire(timeout=timeout), self._state)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


def _scoped_dsn(pg_dsn: str, schema: str) -> str:
    parsed = urlparse(pg_dsn)
    query = (
        f"application_name={schema}"
        if not parsed.query
        else f"{parsed.query}&application_name={schema}"
    )
    return urlunparse(parsed._replace(query=query))


def _settings(pg_dsn: str, schema: str) -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {
            "pg_dsn": pg_dsn,
            "schema_name": schema,
            "heartbeat_interval": "0.5",
            "lock_lease": "5",
            "sweep_interval": "1",
            "poll_interval": "0.05",
            "cancellation_grace_period": "1",
            "cleanup_grace_period": "1",
            "heartbeat_command_timeout": "0.1",
            "watchdog_loop_lag_budget": "1.2",
            "watchdog_loop_lag_warn_budget": "0.5",
            "max_concurrency": "4",
            "queues": [_QUEUE],
            "health_socket_path": unique_health_sock_path("issue402"),
        }
    )


async def _job_rows(conn: asyncpg.Connection, schema: str) -> list[Any]:
    return await conn.fetch(
        f"SELECT id::text, status::text AS status, attempt, claim_epoch, "
        f"locked_by_worker::text AS locked_by, lock_expires_at, started_at "
        f'FROM "{schema}".jobs WHERE tags @> ARRAY[{_TAG!r}::text] '
        f"ORDER BY created_at, id"
    )


async def test_dispatch_round_that_raises_after_its_claim_commit_recovers(
    pg_dsn: str,
    module_pg_schema: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    schema = module_pg_schema.schema_name
    conn = await asyncpg.connect(pg_dsn)
    await conn.close()  # the module fixture applied the migrations.

    state = _InjectionState()

    # The injection wraps the backend's dispatcher pool at the exact seam
    # the issue names: the round's pool release, after the claim commit.
    import taskq.backend.postgres as pg_backend_module

    # The seam IS the module-private alias the backend delegates through;
    # the same private-seam rationale the producer loop's _disown_job
    # import carries.
    real_dispatch = pg_backend_module._dispatch  # pyright: ignore[reportPrivateImportUsage]

    async def injecting_dispatch(dispatcher_pool: Any, *args: Any, **kwargs: Any) -> Any:
        return await real_dispatch(_PoolProxy(dispatcher_pool, state), *args, **kwargs)

    monkeypatch.setattr(pg_backend_module, "_dispatch", injecting_dispatch)

    settings = _settings(_scoped_dsn(pg_dsn, schema), schema)

    async def _runner() -> int:
        with contextlib.suppress(asyncio.CancelledError):
            return await _main(settings, actor_registry=_REGISTRY)
        return 0

    worker_task = asyncio.create_task(_runner(), name="issue402-worker")
    probe = await asyncpg.connect(pg_dsn)
    try:
        ids: list[str] = []
        for index in range(2):
            rows = await probe.fetch(
                f'INSERT INTO "{schema}".jobs '
                "(id, actor, queue, payload, status, max_attempts, retry_kind, "
                "scheduled_at, tags) VALUES (gen_random_uuid(), 'issue402_ok', "
                f"'{_QUEUE}', $1::jsonb, 'pending', 3, 'transient', "
                f"clock_timestamp(), ARRAY[{_TAG!r}::text]) RETURNING id::text",
                Issue402Payload(marker=f"m{index}").model_dump_json(),
            )
            ids.append(rows[0]["id"])

        # Wait for the injected round: the claim commits, then the
        # release raises.
        deadline = asyncio.get_running_loop().time() + 20.0
        while not state.raised:
            await asyncio.sleep(0.05)
            assert asyncio.get_running_loop().time() < deadline, (
                "the injected round never ran: no claim landed within 20s"
            )
        assert state.claimed, "the raise must land after a committed claim"
        # The canary, inside the patched path: the claim the injection
        # raised on returned THIS test's jobs. An injection that fired on
        # some other round's rows would strand nothing this test tracks,
        # and every phase-1 assertion below would be vacuous.
        assert set(ids) <= set(state.claimed_ids), (
            "the injected round must have claimed the tracked jobs: the "
            f"injection saw {state.claimed_ids}, the test enqueued {ids}"
        )

        # ── Phase 1: the stranded shape, exactly as the issue measured ──
        await asyncio.sleep(_STRANDED_OBSERVE_SECS)
        rows = await _job_rows(probe, schema)
        stranded = {r["id"]: r for r in rows if r["id"] in ids}
        assert all(r["status"] == "running" for r in stranded.values()), (
            f"phase 1: the claimed rows must be running after the raise: {stranded}"
        )
        assert all(r["locked_by"] is not None for r in stranded.values()), (
            f"phase 1: the claimed rows must be locked to the worker: {stranded}"
        )
        # The lease is actively renewed (the heartbeat's renewal predicate
        # excludes only the disowned set): sample lock_expires_at twice.
        first_expire = {r["id"]: r["lock_expires_at"] for r in stranded.values()}
        await asyncio.sleep(1.0)
        rows = await _job_rows(probe, schema)
        still_running = {r["id"]: r for r in rows if r["id"] in ids}
        assert all(r["status"] == "running" for r in still_running.values()), (
            f"phase 1: rows must still be running mid-window: {still_running}"
        )
        renewed = {
            rid for rid, r in still_running.items() if r["lock_expires_at"] > first_expire[rid]
        }
        assert renewed, (
            f"phase 1: the stranded rows' leases must be actively renewed "
            f"(the issue's mechanism: the heartbeat keeps them alive): {still_running}"
        )

        # ── Phase 2: bounded recovery ──
        # Bound: probe grace (one lease on started_at) + one lease lapse
        # + sweep interval + re-claim. 30 s is 3x the sum at these
        # settings.
        deadline = asyncio.get_running_loop().time() + _RECOVERY_BOUND_SECS
        while True:
            rows = await _job_rows(probe, schema)
            tracked = [r for r in rows if r["id"] in ids]
            if all(r["status"] == "succeeded" for r in tracked):
                break
            assert asyncio.get_running_loop().time() < deadline, (
                f"LOST JOBS: the claimed batch did not recover within "
                f"{_RECOVERY_BOUND_SECS}s - the rows are stranded running "
                f"under a live worker: {tracked}"
            )
            await asyncio.sleep(0.5)

        # The recovery is a RE-DISPATCH, not a silent terminalization:
        # each row's attempt advanced past the stranded claim's attempt.
        rows = await _job_rows(probe, schema)
        for r in rows:
            if r["id"] in ids:
                assert r["attempt"] >= 2, (
                    f"the recovered job must show a second attempt "
                    f"(re-claimed after the reconcile), got attempt={r['attempt']}"
                )
    finally:
        await probe.close()
        if not worker_task.done():
            worker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, BaseException):
                await asyncio.wait_for(worker_task, timeout=60.0)

"""Issue #104 — moving an actor between queues is ONE atomic operator action,
and worker boot stays consistent at every intermediate state of the rolling
deploy that ships the matching ``@actor(queue=...)`` literal.

Pre-fix failure mode these pins replace: a move took four coordinated writes —
the ``@actor`` literal, the stored ``actor_config.queue`` row, the worker's
consumed-queue set, and the ``queues`` row for the target — and the first pair
was fail-closed: whichever order the operator picked, one side of a rolling
deploy could not boot (``ActorConfigDriftList`` from ``sync_actor_config``)
until the other write landed, while the stranding half (a queue nobody
consumes, a missing ``queues`` row degrading round_robin to strict_fifo) was
silent. The move therefore is:

* one operator action in two phases — the actor's pending/scheduled backlog
  is rewritten onto the target queue as bounded committed batches (the
  deregister force-drain doctrine: windowed CTE, per-batch
  statement_timeout, termination on the window count), then ONE final
  transaction locks the assignment row, carries the source queue's
  ``queues`` row (mode + max_concurrent) to the target when the target has
  none, and flips the stored assignment. Running jobs are untouched: their
  queue is inert once claimed. The flip lands last so a crash mid-drain
  re-runs cleanly (the drain's queue predicate skips rows earlier batches
  moved);
* boot semantics that hold across the window — the stored queue is the
  operator-owned assignment once a row exists (a differing literal logs
  ``actor-config-queue-override`` and boots; the startup UPSERT never
  rewrites the stored queue), so old-literal and new-literal workers both boot
  at every intermediate state. Metadata stays fail-closed: no operator surface
  moves it, so a mismatch there is always a bug.

Unit tier uses a fake connection; integration tier runs against a real
migrated schema (``module_pg_schema``) through the production enqueue and
dispatch paths.
"""

# ruff: noqa: S608  # Why: schema is fixture-derived (module_pg_schema), not user input; values are $-bound.

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg
import pytest
import structlog
from typer.testing import CliRunner

from taskq._ids import new_job_id, new_uuid
from taskq.actor_config import ActorConfig
from taskq.actor_config_ops import ActorQueueMoveResult, move_actor_queue
from taskq.backend._dispatch_sql import DISPATCH_STRICT_FIFO_SQL
from taskq.backend._dispatch_sql import dispatch_batch as dispatch_batch_sql
from taskq.backend._protocol import EnqueueArgs
from taskq.cli import app
from taskq.exceptions import ActorConfigDriftList, ActorNotFoundError
from taskq.testing.fixtures import ModulePgSchema
from taskq.worker.queue_ops import set_queue_max_concurrent, set_queue_mode
from taskq.worker.startup import sync_actor_config

from .test_rt_cron_harness import cron_settings, make_backend

runner = CliRunner()

_LEASE = timedelta(seconds=30)
_OLD_QUEUE = "tqm_old"
_NEW_QUEUE = "tqm_new"
_ACTOR = "tqm_actor"
_OTHER_ACTOR = "tqm_other"
# A fixed past instant for due rows, and a genuinely future one for the
# scheduled row — enqueue stamps 'scheduled' only when the delay from the
# server clock is positive, so a "future" date in the past would silently
# produce another pending row.
_DUE = datetime(2020, 1, 1, tzinfo=UTC)
_FUTURE = datetime.now(UTC) + timedelta(hours=1)


# ═══════════════════════════════════════════════════════════════════════════════
# Unit tier (fake connection): boot consistency across the move window
# ═══════════════════════════════════════════════════════════════════════════════


@dataclass
class _FakeRecord:
    """Record-like double supporting dict-style access, as ``ConnLike`` reads it."""

    _fields: dict[str, object] = field(default_factory=dict[str, object])

    def __getitem__(self, key: str) -> object:
        return self._fields[key]


class _FakeConn:
    """Test double for ``asyncpg.Connection`` recording SELECT/UPSERT calls."""

    def __init__(self) -> None:
        self._select_rows: list[_FakeRecord] = []
        self.execute_calls: list[tuple[str, list[Any]]] = []

    def set_select_rows(self, rows: list[_FakeRecord]) -> None:
        self._select_rows = list(rows)

    async def fetch(self, query: str, *params: Any) -> list[_FakeRecord]:
        return list(self._select_rows)

    async def execute(self, query: str, *params: Any) -> str:
        self.execute_calls.append((query, list(params)))
        return "OK"

    def transaction(self) -> _FakeTransaction:
        return _FakeTransaction()


class _FakeTransaction:
    def __init__(self) -> None:
        self._entered = False

    async def __aenter__(self) -> _FakeTransaction:
        self._entered = True
        return self

    async def __aexit__(self, *args: object) -> None:
        if not self._entered:
            raise RuntimeError("transaction exited without entering")


def _stored_row(
    actor: str, *, queue: str, metadata: dict[str, object] | None = None
) -> _FakeRecord:
    return _FakeRecord(
        {
            "actor": actor,
            "max_concurrent": None,
            "max_pending": None,
            "queue": queue,
            "result_ttl": None,
            "metadata": json.dumps(metadata if metadata is not None else {}),
        }
    )


def _config(actor: str, *, queue: str, metadata: dict[str, object] | None = None) -> ActorConfig:
    return ActorConfig(
        actor=actor,
        max_concurrent=None,
        max_pending=None,
        queue=queue,
        result_ttl=None,
        metadata=metadata if metadata is not None else {},
    )


async def test_mid_transition_boot_succeeds_and_preserves_assignment() -> None:
    """The mid-transition state — stored row already moved to the new queue,
    worker still carrying the OLD code literal — must boot, and its startup
    UPSERT must not flip the stored assignment back. Pre-fix this exact state
    raised ``ActorConfigDriftList`` and refused boot."""
    fake_conn = _FakeConn()
    fake_conn.set_select_rows([_stored_row(_ACTOR, queue=_NEW_QUEUE)])

    with structlog.testing.capture_logs() as logs:
        await sync_actor_config(
            fake_conn,  # pyright: ignore[reportArgumentType]  # Why: unit-test double; real asyncpg.Connection subtyping would need protocol-level mocking
            [_config(_ACTOR, queue=_OLD_QUEUE)],
            force=False,
        )

    # Booted: the UPSERT ran (no refusal, no force flag needed).
    assert len(fake_conn.execute_calls) == 1
    # The assignment is preserved: the conflict clause never rewrites the
    # stored queue, so a stale-literal worker cannot undo the move.
    sql, _params = fake_conn.execute_calls[0]
    on_conflict = sql.split("DO UPDATE SET", 1)[1]
    assert "queue" not in on_conflict
    # Relocated detection: the mismatch is a named, warn-level event.
    override = [e for e in logs if e["event"] == "actor-config-queue-override"]
    assert override, "queue mismatch must be surfaced as actor-config-queue-override"
    assert override[0]["registered"] == _OLD_QUEUE
    assert override[0]["stored"] == _NEW_QUEUE


async def test_queue_only_mismatch_never_refuses_but_metadata_drift_still_does() -> None:
    """A queue mismatch alone never blocks boot (it is the move window, or a
    literal that has not followed the assignment yet). Metadata drift stays
    fail-closed — no operator surface moves metadata, so a mismatch there is
    always a bug and raises exactly as before."""
    fake_conn = _FakeConn()
    fake_conn.set_select_rows([_stored_row(_ACTOR, queue=_NEW_QUEUE)])
    await sync_actor_config(
        fake_conn,  # pyright: ignore[reportArgumentType]  # Why: unit-test double; see above
        [_config(_ACTOR, queue=_OLD_QUEUE)],
        force=False,
    )
    assert len(fake_conn.execute_calls) == 1

    fake_conn = _FakeConn()
    fake_conn.set_select_rows([_stored_row(_ACTOR, queue=_NEW_QUEUE, metadata={"team": "ops"})])
    with pytest.raises(ActorConfigDriftList) as exc_info:
        await sync_actor_config(
            fake_conn,  # pyright: ignore[reportArgumentType]  # Why: unit-test double; see above
            [_config(_ACTOR, queue=_OLD_QUEUE, metadata={"team": "platform"})],
            force=False,
        )
    assert {d.field for d in exc_info.value.drifts} == {"metadata"}


# ═══════════════════════════════════════════════════════════════════════════════
# Integration tier (real migrated schema): the one-step move
# ═══════════════════════════════════════════════════════════════════════════════


async def _enqueue(
    conn: asyncpg.Connection,
    schema: str,
    *,
    actor: str,
    queue: str,
    count: int = 1,
    scheduled_at: datetime = _DUE,
) -> None:
    """Enqueue due (or scheduled) jobs through the production path, pool-free."""
    settings = cron_settings(schema)
    backend = make_backend(settings)
    args = [
        EnqueueArgs(
            id=new_job_id(),
            actor=actor,
            queue=queue,
            payload={"probe": actor},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=scheduled_at,
        )
        for _ in range(count)
    ]
    await backend.enqueue_batch(args, connection=conn)


async def _dispatch(
    conn: asyncpg.Connection, schema: str, queues: list[str], limit_n: int
) -> list[asyncpg.Record]:
    """One dispatch batch via the narrowest entry point: the SQL helper."""
    return await dispatch_batch_sql(
        conn,
        sql=DISPATCH_STRICT_FIFO_SQL.format(schema=schema),
        queues=queues,
        limit_n=limit_n,
        worker_id=new_uuid(),
        lock_lease=_LEASE,
    )


async def _count_jobs(
    conn: asyncpg.Connection,
    schema: str,
    *,
    actor: str,
    queue: str,
    status: str,
) -> int:
    total: object = await conn.fetchval(
        f'SELECT count(*) FROM "{schema}".jobs WHERE actor = $1 AND queue = $2 AND status::text = $3',
        actor,
        queue,
        status,
    )
    return int(total or 0)


class TestMoveActorQueue:
    """The one-transaction move and the boot window it must survive."""

    pytestmark = pytest.mark.integration

    async def test_rewrites_assignment_backlog_and_queue_row_in_one_step(
        self, clean_pg_conn: asyncpg.Connection, module_pg_schema: ModulePgSchema
    ) -> None:
        schema = module_pg_schema.schema_name
        conn = clean_pg_conn

        # Steady state: the actor (and a neighbor) live on the old queue, which
        # is operator-configured round_robin with a cap — the config a naive
        # move silently loses.
        await sync_actor_config(
            conn,
            [
                ActorConfig(actor=_ACTOR, max_concurrent=3, queue=_OLD_QUEUE),
                ActorConfig(actor=_OTHER_ACTOR, max_concurrent=None, queue=_OLD_QUEUE),
            ],
            schema=schema,
        )
        await set_queue_mode(conn, _OLD_QUEUE, "round_robin", schema=schema)
        await set_queue_max_concurrent(conn, _OLD_QUEUE, 4, schema=schema)

        # 3 due + 1 scheduled for the actor; one due job is claimed (running)
        # BEFORE the neighbor's job exists, so the claim is deterministic.
        await _enqueue(conn, schema, actor=_ACTOR, queue=_OLD_QUEUE, count=3)
        await _enqueue(conn, schema, actor=_ACTOR, queue=_OLD_QUEUE, count=1, scheduled_at=_FUTURE)
        claimed = await _dispatch(conn, schema, [_OLD_QUEUE], 1)
        assert len(claimed) == 1 and claimed[0]["actor"] == _ACTOR
        await _enqueue(conn, schema, actor=_OTHER_ACTOR, queue=_OLD_QUEUE, count=1)

        result = await move_actor_queue(conn, _ACTOR, _NEW_QUEUE, schema=schema)

        assert result.actor == _ACTOR
        assert result.from_queue == _OLD_QUEUE
        assert result.to_queue == _NEW_QUEUE
        assert result.jobs_moved == 3  # 2 pending + 1 scheduled
        assert result.running_jobs_left == 1
        assert result.queues_row_carried is True

        # The assignment moved; capacity survives untouched.
        row = await conn.fetchrow(
            f'SELECT queue, max_concurrent FROM "{schema}".actor_config WHERE actor = $1',
            _ACTOR,
        )
        assert row is not None
        assert row["queue"] == _NEW_QUEUE
        assert row["max_concurrent"] == 3

        # The backlog followed: pending AND scheduled rows now sit on the new
        # queue; the claimed (running) job did not; the neighbor's job did not.
        assert (
            await _count_jobs(conn, schema, actor=_ACTOR, queue=_NEW_QUEUE, status="pending") == 2
        )
        assert (
            await _count_jobs(conn, schema, actor=_ACTOR, queue=_NEW_QUEUE, status="scheduled") == 1
        )
        assert (
            await _count_jobs(conn, schema, actor=_ACTOR, queue=_OLD_QUEUE, status="running") == 1
        )
        assert (
            await _count_jobs(conn, schema, actor=_OTHER_ACTOR, queue=_OLD_QUEUE, status="pending")
            == 1
        )
        assert (
            await _count_jobs(conn, schema, actor=_OTHER_ACTOR, queue=_NEW_QUEUE, status="pending")
            == 0
        )

        # The target queue inherited the source queue's row; the source row
        # itself is untouched (the neighbor still lives there).
        new_q = await conn.fetchrow(
            f'SELECT mode, max_concurrent FROM "{schema}".queues WHERE name = $1',
            _NEW_QUEUE,
        )
        assert new_q is not None
        assert new_q["mode"] == "round_robin"
        assert new_q["max_concurrent"] == 4
        old_q = await conn.fetchrow(
            f'SELECT mode, max_concurrent FROM "{schema}".queues WHERE name = $1',
            _OLD_QUEUE,
        )
        assert old_q is not None
        assert old_q["mode"] == "round_robin"
        assert old_q["max_concurrent"] == 4

    async def test_mid_transition_boots_both_sides_and_strays_drain(
        self, clean_pg_conn: asyncpg.Connection, module_pg_schema: ModulePgSchema
    ) -> None:
        """The rolling-deploy window end to end: after the move, a worker with
        the OLD literal boots without refusing (and without undoing the
        assignment), a worker with the NEW literal boots, and the moved
        backlog drains through the NEW queue's consumers."""
        schema = module_pg_schema.schema_name
        conn = clean_pg_conn

        await sync_actor_config(
            conn,
            [ActorConfig(actor=_ACTOR, max_concurrent=None, queue=_OLD_QUEUE)],
            schema=schema,
        )
        await _enqueue(conn, schema, actor=_ACTOR, queue=_OLD_QUEUE, count=3)

        await move_actor_queue(conn, _ACTOR, _NEW_QUEUE, schema=schema)

        # Old-literal worker boots mid-window — pre-fix: ActorConfigDriftList.
        await sync_actor_config(
            conn,
            [ActorConfig(actor=_ACTOR, max_concurrent=None, queue=_OLD_QUEUE)],
            schema=schema,
        )
        # New-literal worker boots.
        await sync_actor_config(
            conn,
            [ActorConfig(actor=_ACTOR, max_concurrent=None, queue=_NEW_QUEUE)],
            schema=schema,
        )
        # Neither boot undid the assignment.
        stored: object = await conn.fetchval(
            f'SELECT queue FROM "{schema}".actor_config WHERE actor = $1', _ACTOR
        )
        assert stored == _NEW_QUEUE

        # Old-queue strays drain: a consumer of ONLY the new queue claims the
        # whole moved backlog.
        claimed = await _dispatch(conn, schema, [_NEW_QUEUE], 10)
        assert len(claimed) == 3
        assert all(r["actor"] == _ACTOR for r in claimed)
        assert all(r["queue"] == _NEW_QUEUE for r in claimed)

    async def test_refusals(
        self, clean_pg_conn: asyncpg.Connection, module_pg_schema: ModulePgSchema
    ) -> None:
        schema = module_pg_schema.schema_name
        conn = clean_pg_conn

        with pytest.raises(ActorNotFoundError):
            await move_actor_queue(conn, "tqm_ghost", _NEW_QUEUE, schema=schema)

        await sync_actor_config(
            conn,
            [ActorConfig(actor=_ACTOR, max_concurrent=None, queue=_OLD_QUEUE)],
            schema=schema,
        )
        # A move onto the queue the actor already occupies is a no-op the
        # operator must be told about, not silently executed.
        with pytest.raises(ValueError, match="already"):
            await move_actor_queue(conn, _ACTOR, _OLD_QUEUE, schema=schema)
        # Queue names follow the same rule every producer follows.
        with pytest.raises(ValueError):
            await move_actor_queue(conn, _ACTOR, "bad:name", schema=schema)

    async def test_never_clobbers_an_existing_target_queue_row(
        self, clean_pg_conn: asyncpg.Connection, module_pg_schema: ModulePgSchema
    ) -> None:
        """A configured target queue stands: the carry is fill-in for an
        unconfigured target, never an overwrite of the operator's own setup."""
        schema = module_pg_schema.schema_name
        conn = clean_pg_conn

        await sync_actor_config(
            conn,
            [ActorConfig(actor=_ACTOR, max_concurrent=None, queue=_OLD_QUEUE)],
            schema=schema,
        )
        await set_queue_mode(conn, _OLD_QUEUE, "round_robin", schema=schema)
        await set_queue_max_concurrent(conn, _OLD_QUEUE, 4, schema=schema)
        # The target is deliberately configured differently.
        await set_queue_mode(conn, _NEW_QUEUE, "strict_fifo", schema=schema)
        await set_queue_max_concurrent(conn, _NEW_QUEUE, 9, schema=schema)

        result = await move_actor_queue(conn, _ACTOR, _NEW_QUEUE, schema=schema)

        assert result.queues_row_carried is False
        new_q = await conn.fetchrow(
            f'SELECT mode, max_concurrent FROM "{schema}".queues WHERE name = $1',
            _NEW_QUEUE,
        )
        assert new_q is not None
        assert new_q["mode"] == "strict_fifo"
        assert new_q["max_concurrent"] == 9


# ═══════════════════════════════════════════════════════════════════════════════
# CLI tier: `taskq actor-config move-queue ACTOR NEW_QUEUE`
# ═══════════════════════════════════════════════════════════════════════════════


def _patch_move(
    monkeypatch: pytest.MonkeyPatch, result: Any = None, exc: BaseException | None = None
) -> None:
    """Fake the move at the taskq.cli boundary, the diff tests' pattern."""

    async def fake_connect(dsn: str) -> Any:
        class _FakeConn:
            async def close(self) -> None: ...

        return _FakeConn()

    async def fake_move(conn: Any, actor: str, new_queue: str, **kwargs: Any) -> Any:
        if exc is not None:
            raise exc
        return result

    monkeypatch.setattr("taskq.cli.asyncpg.connect", fake_connect)
    monkeypatch.setattr("taskq.cli.move_actor_queue", fake_move, raising=False)


def test_cli_move_queue_reports_result(monkeypatch: pytest.MonkeyPatch) -> None:
    moved = ActorQueueMoveResult(
        actor=_ACTOR,
        from_queue=_OLD_QUEUE,
        to_queue=_NEW_QUEUE,
        jobs_moved=3,
        running_jobs_left=1,
        queues_row_carried=True,
    )
    _patch_move(monkeypatch, result=moved)

    result = runner.invoke(app, ["actor-config", "move-queue", _ACTOR, _NEW_QUEUE])

    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert "tqm_old" in result.output
    assert "tqm_new" in result.output
    assert "jobs_moved=3" in result.output
    # The irreducible producer-side residual the operator must plan for.
    assert "tqm_old" in result.stderr


def test_cli_move_queue_unknown_actor_exit_3(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_move(monkeypatch, exc=ActorNotFoundError("tqm_ghost"))

    result = runner.invoke(app, ["actor-config", "move-queue", "tqm_ghost", _NEW_QUEUE])

    assert result.exit_code == 3


def test_cli_move_queue_refusal_exit_2(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_move(monkeypatch, exc=ValueError("already assigned"))

    result = runner.invoke(app, ["actor-config", "move-queue", _ACTOR, _OLD_QUEUE])

    assert result.exit_code == 2
    assert "already assigned" in result.stderr

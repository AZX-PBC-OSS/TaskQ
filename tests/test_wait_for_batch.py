"""Tests for wait_for_batch.

Unit tests use the InMemoryBackend variant from
``taskq.testing.in_memory``. Integration tests use the PG
variant from ``taskq.batch`` against a live Postgres container.
Integration test classes are marked ``@pytest.mark.integration``
individually so the file mixes unit and integration tiers.
"""

import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import asyncpg
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from pydantic import BaseModel

from taskq import actor
from taskq._ids import new_base62
from taskq._json import dumps_str
from taskq.batch import BatchCompletionStatus, EnqueueItem, wait_for_batch
from taskq.exceptions import Snooze
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.in_memory import wait_for_batch as in_memory_wait_for_batch
from taskq.testing.jobs import make_job_row

_CLOCK_START = datetime(2025, 1, 1, tzinfo=UTC)


class _Payload(BaseModel):
    value: int = 0


@actor(name="wfb_test_actor")
async def _test_actor(_payload: _Payload) -> None:
    pass


def _make_backend() -> InMemoryBackend:
    return InMemoryBackend(clock=FakeClock(start=_CLOCK_START))


def _make_item(value: int = 0) -> EnqueueItem:
    return EnqueueItem(actor_ref=_test_actor, payload=_Payload(value=value))


def _seed_batch(
    backend: InMemoryBackend,
    n: int,
    batch_id: UUID,
    status: str = "pending",
) -> list[UUID]:
    """Insert *n* jobs into the backend with the given batch_id and status."""
    job_ids: list[UUID] = []
    for _i in range(n):
        row = make_job_row(
            actor=_test_actor.name,
            status=status,  # type: ignore[arg-type] # Why: make_job_row expects JobStatus Literal; seed function receives str from parameter
        )
        row = replace(row, metadata={"batch_id": str(batch_id)})
        backend._jobs[row.id] = row  # type: ignore[reportPrivateUsage] # Why: test-only direct store access to seed batch jobs with specific statuses
        job_ids.append(row.id)
    return job_ids


async def _setup_pg_schema(pg_dsn: str, schema: str) -> None:
    """Drop-and-recreate *schema*, apply migrations."""
    from taskq.migrate import apply_pending

    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema)
    finally:
        await conn.close()


async def _enqueue_batch_pg(
    pg_dsn: str,
    schema: str,
    n: int,
    batch_id: UUID,
) -> None:
    """Enqueue *n* items into PG under *batch_id* via TaskQ context."""
    from taskq import TaskQ

    async with TaskQ(dsn=pg_dsn, schema=schema) as tq:
        items = [_make_item(i) for i in range(n)]
        await tq.enqueue_batch(items, batch_id=batch_id)


# ── All children terminal ──────────────────────────────────────


class TestAllChildrenTerminal:
    """All children terminal — returns BatchCompletionStatus, no Snooze."""

    async def test_all_succeeded(self) -> None:
        backend = _make_backend()
        batch_id = uuid4()
        _seed_batch(backend, 3, batch_id, status="succeeded")

        status = await in_memory_wait_for_batch(backend, batch_id)

        assert status == BatchCompletionStatus(
            total=3, pending=0, succeeded=3, failed=0, cancelled=0, crashed=0, abandoned=0
        )
        assert status.is_complete is True


# ── One child in-flight ─────────────────────────────────────────


class TestOneChildInFlight:
    """One child in-flight — raises Snooze with default interval."""

    async def test_raises_snooze(self) -> None:
        backend = _make_backend()
        batch_id = uuid4()
        _seed_batch(backend, 2, batch_id, status="succeeded")
        _seed_batch(backend, 1, batch_id, status="pending")

        with pytest.raises(Snooze) as exc_info:
            await in_memory_wait_for_batch(backend, batch_id)

        assert exc_info.value.delay == timedelta(seconds=10)


# ── Custom snooze_interval propagates ───────────────────────────


class TestCustomSnoozeInterval:
    """Custom snooze_interval propagates into Snooze exception."""

    async def test_custom_interval_in_snooze(self) -> None:
        backend = _make_backend()
        batch_id = uuid4()
        _seed_batch(backend, 1, batch_id, status="pending")

        with pytest.raises(Snooze) as exc_info:
            await in_memory_wait_for_batch(backend, batch_id, snooze_interval=timedelta(seconds=30))

        assert exc_info.value.delay == timedelta(seconds=30)


# ── Mixed terminal statuses with one in-flight ──────────────────


class TestMixedTerminalWithInFlight:
    """Mixed terminal + one running — still raises Snooze."""

    async def test_mixed_statuses_raises_snooze(self) -> None:
        backend = _make_backend()
        batch_id = uuid4()
        _seed_batch(backend, 2, batch_id, status="succeeded")
        _seed_batch(backend, 1, batch_id, status="failed")
        _seed_batch(backend, 1, batch_id, status="cancelled")
        _seed_batch(backend, 1, batch_id, status="running")

        with pytest.raises(Snooze):
            await in_memory_wait_for_batch(backend, batch_id)


# ── All terminal with failures ──────────────────────────────────


class TestAllTerminalWithFailures:
    """All terminal with failures — returns correct counts."""

    async def test_succeeded_and_failed(self) -> None:
        backend = _make_backend()
        batch_id = uuid4()
        _seed_batch(backend, 2, batch_id, status="succeeded")
        _seed_batch(backend, 1, batch_id, status="failed")

        status = await in_memory_wait_for_batch(backend, batch_id)

        assert status == BatchCompletionStatus(
            total=3, pending=0, succeeded=2, failed=1, cancelled=0, crashed=0, abandoned=0
        )
        assert status.is_complete is True


# ── Empty batch_id ──────────────────────────────────────────────


class TestEmptyBatchId:
    """Empty batch_id — returns total=0 with is_complete=True."""

    async def test_empty_batch_returns_zero(self) -> None:
        backend = _make_backend()
        batch_id = uuid4()

        status = await in_memory_wait_for_batch(backend, batch_id)

        assert status == BatchCompletionStatus(
            total=0, pending=0, succeeded=0, failed=0, cancelled=0, crashed=0, abandoned=0
        )
        assert status.is_complete is True


# ── snooze_interval clamped ─────────────────────────────────────


class TestSnoozeIntervalClamped:
    """snooze_interval=500ms clamped to 1s."""

    async def test_sub_second_clamped(self) -> None:
        backend = _make_backend()
        batch_id = uuid4()
        _seed_batch(backend, 1, batch_id, status="pending")

        with pytest.raises(Snooze) as exc_info:
            await in_memory_wait_for_batch(
                backend, batch_id, snooze_interval=timedelta(milliseconds=500)
            )

        assert exc_info.value.delay == timedelta(seconds=1)


# ── snooze_interval=0 clamped ───────────────────────────────────


class TestSnoozeIntervalZeroClamped:
    """snooze_interval=0 clamped to 1s."""

    async def test_zero_clamped(self) -> None:
        backend = _make_backend()
        batch_id = uuid4()
        _seed_batch(backend, 1, batch_id, status="pending")

        with pytest.raises(Snooze) as exc_info:
            await in_memory_wait_for_batch(backend, batch_id, snooze_interval=timedelta(0))

        assert exc_info.value.delay == timedelta(seconds=1)


# ── Abandoned child is terminal ──────────────────────────────────


class TestAbandonedChildIsTerminal:
    """Abandoned child is terminal — pending drops to 0."""

    async def test_abandoned_counts_as_terminal(self) -> None:
        backend = _make_backend()
        batch_id = uuid4()
        _seed_batch(backend, 2, batch_id, status="succeeded")
        _seed_batch(backend, 1, batch_id, status="abandoned")

        status = await in_memory_wait_for_batch(backend, batch_id)

        assert status == BatchCompletionStatus(
            total=3, pending=0, succeeded=2, failed=0, cancelled=0, crashed=0, abandoned=1
        )
        assert status.is_complete is True


# ── Crashed child is terminal ───────────────────────────────────


class TestCrashedChildIsTerminal:
    """Crashed child is terminal — pending drops to 0."""

    async def test_crashed_counts_as_terminal(self) -> None:
        backend = _make_backend()
        batch_id = uuid4()
        _seed_batch(backend, 2, batch_id, status="succeeded")
        _seed_batch(backend, 1, batch_id, status="crashed")

        status = await in_memory_wait_for_batch(backend, batch_id)

        assert status == BatchCompletionStatus(
            total=3, pending=0, succeeded=2, failed=0, cancelled=0, crashed=1, abandoned=0
        )
        assert status.is_complete is True


# ── Batch completion invariant (Hypothesis) ─────────────────────


class TestBatchCompletionInvariant:
    """Batch completion invariant — Snooze iff any non-terminal."""

    @given(
        statuses=st.lists(
            st.sampled_from(
                [
                    "pending",
                    "scheduled",
                    "running",
                    "succeeded",
                    "failed",
                    "cancelled",
                    "crashed",
                    "abandoned",
                ]
            ),
            min_size=1,
            max_size=20,
        )
    )
    @settings(max_examples=50)
    async def test_invariant(self, statuses: list[str]) -> None:
        backend = _make_backend()
        batch_id = uuid4()
        terminal = {"succeeded", "failed", "cancelled", "crashed", "abandoned"}

        for s in statuses:
            _seed_batch(backend, 1, batch_id, status=s)

        all_terminal = all(s in terminal for s in statuses)
        if all_terminal:
            status = await in_memory_wait_for_batch(backend, batch_id)
            assert status.pending == 0
            assert status.is_complete is True
            assert status.total == len(statuses)
        else:
            with pytest.raises(Snooze):
                await in_memory_wait_for_batch(backend, batch_id)


# ── Full round-trip via actor snooze ─────────────────────────────


@pytest.mark.integration
class TestFullRoundTrip:
    """Full round-trip — Snooze while in-flight, BatchCompletionStatus when all terminal."""

    async def test_snooze_then_complete(self, pg_dsn: str) -> None:
        schema = f"taskq_test_wfb_ti1_{new_base62()}".lower()
        await _setup_pg_schema(pg_dsn, schema)

        batch_id = uuid4()
        await _enqueue_batch_pg(pg_dsn, schema, 3, batch_id)

        conn = await asyncpg.connect(pg_dsn)
        try:
            await conn.execute(
                f'UPDATE "{schema}".jobs SET status = \'succeeded\' WHERE id IN (SELECT id FROM "{schema}".jobs WHERE metadata @> $1::jsonb LIMIT 2)',  # noqa: S608 # Why: schema is a test-local constant; no user input reaches here
                dumps_str({"batch_id": str(batch_id)}),
            )

            with pytest.raises(Snooze):
                await wait_for_batch(conn, batch_id, schema=schema)

            await conn.execute(
                f"UPDATE \"{schema}\".jobs SET status = 'succeeded' WHERE metadata @> $1::jsonb AND status != 'succeeded'",  # noqa: S608 # Why: schema is a test-local constant
                dumps_str({"batch_id": str(batch_id)}),
            )

            status = await wait_for_batch(conn, batch_id, schema=schema)
        finally:
            await conn.close()

        assert status.total == 3
        assert status.succeeded == 3
        assert status.pending == 0
        assert status.is_complete is True


# ── End-to-end with failed child ─────────────────────────────────


@pytest.mark.integration
class TestWithFailedChild:
    """End-to-end with failed child — mixed terminal counts."""

    async def test_succeeded_and_failed(self, pg_dsn: str) -> None:
        schema = f"taskq_test_wfb_ti2_{new_base62()}".lower()
        await _setup_pg_schema(pg_dsn, schema)

        batch_id = uuid4()
        await _enqueue_batch_pg(pg_dsn, schema, 3, batch_id)

        conn = await asyncpg.connect(pg_dsn)
        try:
            await conn.execute(
                f'UPDATE "{schema}".jobs SET status = \'succeeded\' WHERE id IN (SELECT id FROM "{schema}".jobs WHERE metadata @> $1::jsonb LIMIT 2)',  # noqa: S608 # Why: schema is a test-local constant
                dumps_str({"batch_id": str(batch_id)}),
            )
            await conn.execute(
                f"UPDATE \"{schema}\".jobs SET status = 'failed' WHERE metadata @> $1::jsonb AND status = 'pending'",  # noqa: S608 # Why: schema is a test-local constant
                dumps_str({"batch_id": str(batch_id)}),
            )

            status = await wait_for_batch(conn, batch_id, schema=schema)
        finally:
            await conn.close()

        assert status.total == 3
        assert status.pending == 0
        assert status.is_complete is True
        assert status.succeeded == 2
        assert status.failed == 1


# ── GIN index oracle ───────────────────────────────────────────


def _find_index_in_plan(plan: dict[str, object], index_name: str) -> bool:
    """Recursively search EXPLAIN plan for an index scan on *index_name*."""
    node_type = plan.get("Node Type", "")
    index_name_val = plan.get("Index Name", "")
    if "Index Scan" in str(node_type) and index_name in str(index_name_val):
        return True
    for key in ("Plans", "Plan"):
        child = plan.get(key)
        if isinstance(child, list):
            for sub in child:
                if isinstance(sub, dict) and _find_index_in_plan(sub, index_name):
                    return True
        elif isinstance(child, dict) and _find_index_in_plan(child, index_name):
            return True
    return False


@pytest.mark.integration
class TestTUGINinIndexUsed:
    """GIN index used by wait_for_batch SQL."""

    async def test_gin_index_scan_on_containment_query(self, pg_dsn: str) -> None:
        from taskq._json import loads as _json_loads
        from taskq.migrate import apply_pending

        schema = f"taskq_test_wfb_tugin_{new_base62()}".lower()
        conn = await asyncpg.connect(pg_dsn)
        try:
            await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            await apply_pending(conn, schema=schema)
        finally:
            await conn.close()

        batch_ids = [str(uuid4()) for _ in range(10)]
        conn = await asyncpg.connect(pg_dsn)
        try:
            records: list[tuple[object, ...]] = []
            for i in range(5000):
                bid = batch_ids[i % 10]
                records.append(
                    (
                        uuid4(),
                        "seed_actor",
                        "default",
                        "{}",
                        "pending",
                        3,
                        "transient",
                        dumps_str({"batch_id": bid}),
                    )
                )
            await conn.copy_records_to_table(
                "jobs",
                schema_name=schema,
                records=records,
                columns=[
                    "id",
                    "actor",
                    "queue",
                    "payload",
                    "status",
                    "max_attempts",
                    "retry_kind",
                    "metadata",
                ],
            )
        finally:
            await conn.close()

        target_batch = batch_ids[0]
        conn = await asyncpg.connect(pg_dsn)
        try:
            explain_rows = await conn.fetch(
                f"EXPLAIN (ANALYZE, FORMAT JSON) "  # noqa: S608 # Why: schema is a test-local constant; no user input reaches here
                f"SELECT count(*) AS total, "
                f"count(*) FILTER (WHERE status = 'succeeded') AS succeeded, "
                f"count(*) FILTER (WHERE status = 'failed') AS failed, "
                f"count(*) FILTER (WHERE status = 'cancelled') AS cancelled, "
                f"count(*) FILTER (WHERE status = 'crashed') AS crashed, "
                f"count(*) FILTER (WHERE status = 'abandoned') AS abandoned, "
                f"count(*) FILTER (WHERE status NOT IN ('succeeded','failed','cancelled','crashed','abandoned')) AS in_flight "
                f'FROM "{schema}".jobs '
                f"WHERE metadata @> $1::jsonb",
                dumps_str({"batch_id": target_batch}),
            )
        finally:
            await conn.close()

        plan_data = _json_loads(explain_rows[0]["QUERY PLAN"])[0]["Plan"]
        assert _find_index_in_plan(plan_data, "jobs_metadata_gin_idx")


# ── Finalizer snooze-then-complete pattern ───────────────────────


@pytest.mark.integration
class TestFinalizerSnoozePattern:
    """Finalizer pattern — Snooze raised while children in-flight, BatchCompletionStatus returned after completion."""

    async def test_snooze_then_succeed(self, pg_dsn: str) -> None:
        schema = f"taskq_test_wfb_ti3_{new_base62()}".lower()
        await _setup_pg_schema(pg_dsn, schema)

        batch_id = uuid4()
        await _enqueue_batch_pg(pg_dsn, schema, 3, batch_id)

        conn = await asyncpg.connect(pg_dsn)
        try:
            with pytest.raises(Snooze) as exc_info:
                await wait_for_batch(conn, batch_id, schema=schema)
            assert exc_info.value.delay == timedelta(seconds=10)

            await conn.execute(
                f'UPDATE "{schema}".jobs SET status = \'succeeded\' WHERE id IN (SELECT id FROM "{schema}".jobs WHERE metadata @> $1::jsonb LIMIT 2)',  # noqa: S608 # Why: schema is a test-local constant
                dumps_str({"batch_id": str(batch_id)}),
            )

            with pytest.raises(Snooze):
                await wait_for_batch(conn, batch_id, schema=schema)

            await conn.execute(
                f"UPDATE \"{schema}\".jobs SET status = 'succeeded' WHERE metadata @> $1::jsonb AND status != 'succeeded'",  # noqa: S608 # Why: schema is a test-local constant
                dumps_str({"batch_id": str(batch_id)}),
            )

            status = await wait_for_batch(conn, batch_id, schema=schema)
        finally:
            await conn.close()

        assert status.total == 3
        assert status.succeeded == 3
        assert status.pending == 0
        assert status.is_complete is True


# ── Child fails and retries ──────────────────────────────────────


@pytest.mark.integration
class TestChildFailsAndRetries:
    """Child fails then returns to pending — Snooze during retry window."""

    async def test_retry_keeps_pending_positive(self, pg_dsn: str) -> None:
        schema = f"taskq_test_wfb_ti4_{new_base62()}".lower()
        await _setup_pg_schema(pg_dsn, schema)

        batch_id = uuid4()
        await _enqueue_batch_pg(pg_dsn, schema, 2, batch_id)

        conn = await asyncpg.connect(pg_dsn)
        try:
            await conn.execute(
                f'UPDATE "{schema}".jobs SET status = \'succeeded\' WHERE id IN (SELECT id FROM "{schema}".jobs WHERE metadata @> $1::jsonb LIMIT 1)',  # noqa: S608 # Why: schema is a test-local constant
                dumps_str({"batch_id": str(batch_id)}),
            )
            await conn.execute(
                f"UPDATE \"{schema}\".jobs SET status = 'failed' WHERE metadata @> $1::jsonb AND status = 'pending'",  # noqa: S608 # Why: schema is a test-local constant
                dumps_str({"batch_id": str(batch_id)}),
            )

            status_before_retry = await wait_for_batch(conn, batch_id, schema=schema)
            assert status_before_retry.is_complete is True
            assert status_before_retry.succeeded == 1
            assert status_before_retry.failed == 1

            await conn.execute(
                f"UPDATE \"{schema}\".jobs SET status = 'pending' WHERE metadata @> $1::jsonb AND status = 'failed'",  # noqa: S608 # Why: schema is a test-local constant
                dumps_str({"batch_id": str(batch_id)}),
            )

            with pytest.raises(Snooze):
                await wait_for_batch(conn, batch_id, schema=schema)

            await conn.execute(
                f"UPDATE \"{schema}\".jobs SET status = 'succeeded' WHERE metadata @> $1::jsonb AND status != 'succeeded'",  # noqa: S608 # Why: schema is a test-local constant
                dumps_str({"batch_id": str(batch_id)}),
            )
            status = await wait_for_batch(conn, batch_id, schema=schema)
        finally:
            await conn.close()

        assert status.total == 2
        assert status.succeeded == 2
        assert status.pending == 0
        assert status.is_complete is True


# ── Blocking form (snooze_via_exception=False) ──────────────────


@pytest.mark.integration
class TestBlockingForm:
    """snooze_via_exception=False loops via sleep, returns without raising."""

    async def test_blocking_form_returns_on_completion(self, pg_dsn: str) -> None:
        schema = f"taskq_test_wfb_ti5_{new_base62()}".lower()
        await _setup_pg_schema(pg_dsn, schema)

        batch_id = uuid4()
        await _enqueue_batch_pg(pg_dsn, schema, 2, batch_id)

        async def _mark_succeeded() -> None:
            await asyncio.sleep(0.3)
            c = await asyncpg.connect(pg_dsn)
            try:
                await c.execute(
                    f"UPDATE \"{schema}\".jobs SET status = 'succeeded' WHERE metadata @> $1::jsonb",  # noqa: S608 # Why: schema is a test-local constant
                    dumps_str({"batch_id": str(batch_id)}),
                )
            finally:
                await c.close()

        wfb_conn = await asyncpg.connect(pg_dsn)
        try:
            async with asyncio.TaskGroup() as tg:
                tg.create_task(_mark_succeeded())
                status = await wait_for_batch(
                    wfb_conn,
                    batch_id,
                    schema=schema,
                    snooze_via_exception=False,
                    snooze_interval=timedelta(seconds=1),
                )
        finally:
            await wfb_conn.close()

        assert status.is_complete is True
        assert status.total == 2
        assert status.succeeded == 2


# ── Race guard — enqueue-before-wait ordering ───────────────────


@pytest.mark.integration
class TestRaceGuardEnqueueBeforeWait:
    """Race guard — uncommitted batch not visible to wait_for_batch."""

    async def test_uncommitted_batch_invisible(self, pg_dsn: str) -> None:
        schema = f"taskq_test_wfb_ti6_{new_base62()}".lower()
        await _setup_pg_schema(pg_dsn, schema)

        batch_id = uuid4()
        conn_a = await asyncpg.connect(pg_dsn)
        conn_b = await asyncpg.connect(pg_dsn)
        try:
            async with conn_a.transaction():
                await conn_a.execute(
                    f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, status, max_attempts, retry_kind, metadata) '  # noqa: S608 # Why: schema is a test-local constant
                    "VALUES ($1::uuid, 'wfb_test_actor', 'default', '{}'::jsonb, 'pending', 3, 'transient', $2::jsonb)",
                    uuid4(),
                    dumps_str({"batch_id": str(batch_id)}),
                )

                status = await wait_for_batch(conn_b, batch_id, schema=schema)
                assert status.total == 0

            await conn_a.execute(
                f"UPDATE \"{schema}\".jobs SET status = 'succeeded' WHERE metadata @> $1::jsonb",  # noqa: S608 # Why: schema is a test-local constant
                dumps_str({"batch_id": str(batch_id)}),
            )
            status = await wait_for_batch(conn_b, batch_id, schema=schema)
            assert status.total == 1
        finally:
            await conn_a.close()
            await conn_b.close()


# ── Invalid batch_id against PG ──────────────────────────────────


@pytest.mark.integration
class TestInvalidBatchIdAgainstPG:
    """UUID matching no PG rows — returns total=0, is_complete=True."""

    async def test_unknown_batch_id_pg(self, pg_dsn: str) -> None:
        schema = f"taskq_test_wfb_tn1_{new_base62()}".lower()
        await _setup_pg_schema(pg_dsn, schema)

        batch_id = uuid4()
        conn = await asyncpg.connect(pg_dsn)
        try:
            status = await wait_for_batch(conn, batch_id, schema=schema)
        finally:
            await conn.close()

        assert status.total == 0
        assert status.pending == 0
        assert status.is_complete is True


# ── PG unavailable — error propagates ────────────────────────────


@pytest.mark.integration
class TestPGUnavailable:
    """PG connection error propagates — not masked as Snooze."""

    async def test_connection_error_propagates(self) -> None:
        batch_id = uuid4()
        with pytest.raises((OSError, asyncpg.PostgresConnectionError)):
            conn = await asyncpg.connect("postgresql://nonexistent:5432/db")
            try:
                await wait_for_batch(conn, batch_id)
            finally:
                await conn.close()

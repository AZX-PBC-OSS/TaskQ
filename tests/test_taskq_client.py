"""Integration and unit tests for the :class:`~taskq.client.TaskQ` top-level client.

Covers lifecycle (open/close/context-manager), constructor validation, and all
public job operations (enqueue, get, get_row, list, cancel, stream) against a real
Postgres backend.

Test plan IDs map to the spec in the task description:
- Lifecycle: open/close patterns, guard clauses, pool-ownership semantics.
- Enqueue: JobHandle shape, idempotency, scheduled_at status.
- Get: hit and miss.
- Get_row: hit and miss (raw JobRow, no handle machinery).
- List: queue / status / actor filters.
- Cancel: pending job and unknown id.
- Stream: NotImplementedError stub.
"""

import asyncio
from datetime import UTC, datetime, timedelta
from typing import cast
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import asyncpg
import pytest
from dotenvmodel import TypeCoercionError
from dotenvmodel.types import RedisDsn
from pydantic import BaseModel, TypeAdapter

from taskq import TaskQ, actor
from taskq._ids import new_base62, new_job_id
from taskq.backend._enqueue import _enqueue_on_conn
from taskq.backend._protocol import Backend, JobFilter, JobId, JobRow
from taskq.backend._sql_templates import render as render_sql
from taskq.backend.clock import SystemClock
from taskq.client._handle import JobHandle
from taskq.client._taskq import JobEvent, _stream_pg, _stream_redis
from taskq.exceptions import IdempotencyKeyLockTimeoutError
from taskq.migrate import apply_pending
from taskq.testing.jobs import make_enqueue_args
from taskq.types import CancelResult

pytestmark = pytest.mark.integration

_SCHEMA_LABEL = f"ttc_{new_base62()}".lower()

# ---------------------------------------------------------------------------
# Shared test actor
# ---------------------------------------------------------------------------


class _Payload(BaseModel):
    value: int = 1


@actor(name="tq_client_test_actor")
async def _test_actor(_payload: _Payload) -> None:
    pass


# Result adapter for void actors
_RA: TypeAdapter[None] = TypeAdapter(type(None))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _migrate(dsn: str, schema: str = _SCHEMA_LABEL) -> None:
    """Drop the test schema and apply all migrations.

    pg_conn fixture drops the schema but does NOT recreate it - migrations
    must be applied before TaskQ can use the schema.
    """
    conn = await asyncpg.connect(dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema)
    finally:
        await conn.close()


# ---------------------------------------------------------------------------
# TestLifecycle
# ---------------------------------------------------------------------------


class TestLifecycle:
    """TaskQ lifecycle: open/close patterns and guard clauses."""

    async def test_async_with_opens_and_closes_cleanly(self, pg_dsn: str) -> None:
        """async with TaskQ(dsn=...) opens cleanly and closes without error."""
        await _migrate(pg_dsn)
        async with TaskQ(dsn=pg_dsn, schema=_SCHEMA_LABEL) as tq:
            # If we get here the pool is open and the client is live.
            assert tq is not None

    async def test_explicit_open_close(self, pg_dsn: str) -> None:
        """await tq.open() + await tq.close() works (FastAPI lifespan pattern)."""
        await _migrate(pg_dsn)
        tq = TaskQ(dsn=pg_dsn, schema=_SCHEMA_LABEL)
        await tq.open()
        try:
            handle = await tq.enqueue(_test_actor, _Payload(value=7))
            assert isinstance(handle, JobHandle)
        finally:
            await tq.close()

    async def test_open_twice_raises_runtime_error(self, pg_dsn: str) -> None:
        """Calling open() on an already-open TaskQ raises RuntimeError."""
        await _migrate(pg_dsn)
        tq = TaskQ(dsn=pg_dsn, schema=_SCHEMA_LABEL)
        await tq.open()
        try:
            with pytest.raises(RuntimeError, match="already open"):
                await tq.open()
        finally:
            await tq.close()

    async def test_job_method_before_open_raises_runtime_error(self, pg_dsn: str) -> None:
        """Calling enqueue before open() raises RuntimeError referencing tq.open()."""
        tq = TaskQ(dsn=pg_dsn, schema=_SCHEMA_LABEL)
        with pytest.raises(RuntimeError, match=r"tq\.open"):
            await tq.enqueue(_test_actor, _Payload())

    async def test_get_before_open_raises_runtime_error(self, pg_dsn: str) -> None:
        """Calling get() before open() raises RuntimeError."""
        tq = TaskQ(dsn=pg_dsn, schema=_SCHEMA_LABEL)
        with pytest.raises(RuntimeError, match=r"tq\.open"):
            await tq.get(new_job_id(), result_adapter=_RA)

    async def test_list_before_open_raises_runtime_error(self, pg_dsn: str) -> None:
        """Calling list() before open() raises RuntimeError."""
        tq = TaskQ(dsn=pg_dsn, schema=_SCHEMA_LABEL)
        with pytest.raises(RuntimeError, match=r"tq\.open"):
            await tq.list(JobFilter())

    async def test_cancel_before_open_raises_runtime_error(self, pg_dsn: str) -> None:
        """Calling cancel() before open() raises RuntimeError."""
        tq = TaskQ(dsn=pg_dsn, schema=_SCHEMA_LABEL)
        with pytest.raises(RuntimeError, match=r"tq\.open"):
            await tq.cancel(new_job_id())

    async def test_close_when_already_closed_is_noop(self, pg_dsn: str) -> None:
        """close() on a closed (never opened) TaskQ is a no-op - does not raise."""
        tq = TaskQ(dsn=pg_dsn, schema=_SCHEMA_LABEL)
        await tq.close()  # must not raise

    async def test_close_twice_is_noop(self, pg_dsn: str) -> None:
        """Calling close() twice does not raise."""
        await _migrate(pg_dsn)
        tq = TaskQ(dsn=pg_dsn, schema=_SCHEMA_LABEL)
        await tq.open()
        await tq.close()
        await tq.close()  # second close is a no-op

    def test_no_dsn_no_pool_raises_value_error(self) -> None:
        """TaskQ() with neither dsn nor pool raises ValueError at construction."""
        with pytest.raises(ValueError, match=r"dsn.*pool|pool.*dsn"):
            TaskQ()

    def test_both_dsn_and_pool_raises_value_error(self, pg_dsn: str) -> None:
        """TaskQ(dsn=..., pool=...) raises ValueError - they are mutually exclusive."""
        # We only need a pool object for the constructor check; we never open it.
        import unittest.mock as mock

        fake_pool = mock.MagicMock(spec=asyncpg.Pool)
        with pytest.raises(ValueError, match=r"dsn.*pool|pool.*dsn"):
            TaskQ(dsn=pg_dsn, pool=fake_pool)

    def test_both_redis_url_and_redis_client_raises_value_error(self, pg_dsn: str) -> None:
        """TaskQ(redis_url=..., redis_client=...) raises ValueError -
        they are mutually exclusive.
        """
        fake_redis = MagicMock()
        with pytest.raises(ValueError, match=r"redis_url.*redis_client|redis_client.*redis_url"):
            TaskQ(dsn=pg_dsn, redis_url="redis://localhost:6379/0", redis_client=fake_redis)

    def test_poll_timeout_stored(self, pg_dsn: str) -> None:
        """TaskQ(poll_timeout=5.0) stores the value as _poll_timeout."""
        tq = TaskQ(dsn=pg_dsn, poll_timeout=5.0)
        assert tq._poll_timeout == 5.0

    def test_poll_timeout_default(self, pg_dsn: str) -> None:
        """TaskQ() without poll_timeout defaults _poll_timeout to 30.0."""
        tq = TaskQ(dsn=pg_dsn)
        assert tq._poll_timeout == 30.0

    def test_dsn_none_when_pool_supplied(self) -> None:
        """When TaskQ is constructed with pool= (no dsn), _dsn remains None."""
        fake_pool = MagicMock(spec=asyncpg.Pool)
        tq = TaskQ(pool=fake_pool)
        assert tq._dsn is None

    async def test_caller_owned_pool_not_closed_by_taskq(self, pg_dsn: str) -> None:
        """When TaskQ is constructed with a caller-owned pool, close() does not
        close that pool - it remains usable after TaskQ.close().
        """
        await _migrate(pg_dsn)
        # Open a pool that the caller owns.
        pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=2)
        assert pool is not None
        try:
            async with TaskQ(pool=pool, schema=_SCHEMA_LABEL):
                pass  # opens and closes TaskQ but must NOT close the pool

            # Pool should still be usable after TaskQ closed.
            async with pool.acquire() as conn:
                result = await conn.fetchval("SELECT 1")
            assert result == 1
        finally:
            await pool.close()


# ---------------------------------------------------------------------------
# TestSchemaResolution - the client and the fleet read one schema truth
# ---------------------------------------------------------------------------


class TestSchemaResolution:
    """``TaskQ(schema=None)`` resolves the schema the way the worker and CLI
    resolve it: explicit argument, then ``TASKQ_SCHEMA_NAME``, then the model
    default. A client hardwired to ``"taskq"`` while the fleet listens on the
    env-configured schema is a silent job-loss vector - the enqueue succeeds
    into a schema no worker reads and ``wait()`` reports a bare timeout.
    """

    def test_schema_from_env_var_when_not_passed(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """TASKQ_SCHEMA_NAME set, no constructor arg: the client lands in the
        fleet's schema. DOTENV_READ_DOTFILES=false keeps the resolution
        hermetic - a developer's local .env must not decide the outcome."""
        monkeypatch.setenv("TASKQ_SCHEMA_NAME", "adopter_trial")
        monkeypatch.setenv("DOTENV_READ_DOTFILES", "false")

        tq = TaskQ(dsn="postgresql://u:p@localhost:5432/db")

        assert tq._schema == "adopter_trial"

    def test_explicit_schema_wins_over_env_var(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The constructor argument is authoritative - same precedence the
        CLI's ``--schema`` option has over TASKQ_SCHEMA_NAME."""
        monkeypatch.setenv("TASKQ_SCHEMA_NAME", "adopter_trial")

        tq = TaskQ(dsn="postgresql://u:p@localhost:5432/db", schema="explicit_schema")

        assert tq._schema == "explicit_schema"

    def test_schema_defaults_to_taskq_when_unset(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """No arg and no env var: the documented default holds."""
        monkeypatch.delenv("TASKQ_SCHEMA_NAME", raising=False)
        monkeypatch.setenv("DOTENV_READ_DOTFILES", "false")

        tq = TaskQ(dsn="postgresql://u:p@localhost:5432/db")

        assert tq._schema == "taskq"

    async def test_enqueue_lands_in_env_var_schema(
        self, pg_dsn: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """End to end: a schema-less client enqueues into the env-configured
        schema its workers migrated - the finding's reproduced divergence
        (job in `taskq`, worker listening on the adopter's schema) pinned
        shut at the row level."""
        schema = f"ttc_env_{new_base62()}".lower()
        await _migrate(pg_dsn, schema=schema)
        monkeypatch.setenv("TASKQ_SCHEMA_NAME", schema)

        async with TaskQ(dsn=pg_dsn) as tq:
            handle = await tq.enqueue(_test_actor, _Payload(value=1))

        conn = await asyncpg.connect(pg_dsn)
        try:
            count = await conn.fetchval(
                f'SELECT COUNT(*) FROM "{schema}".jobs WHERE id = $1',  # noqa: S608 - schema is a per-test generated identifier (new_base62), not user input; the id is $1-bound
                handle.job_id,
            )
        finally:
            await conn.close()
        assert count == 1


# ---------------------------------------------------------------------------
# TestCloseBounded - owned-pool close is bounded
# ---------------------------------------------------------------------------


class _FakeHungClosePool:
    """Hand-rolled pool stand-in whose close() hangs while close_wait is cleared.

    asyncpg is a C extension - spec-mocks cannot express a hang gate - so
    this mirrors the _FakePool conventions in tests/test_cli_ui.py.
    terminate() releases the gate, mirroring the real Pool whose terminate()
    kills connections immediately.
    """

    def __init__(self) -> None:
        self.close_calls = 0
        self.close_wait = asyncio.Event()
        self.close_wait.set()
        self.closed = False
        self.terminated = False

    async def close(self) -> None:
        self.close_calls += 1
        await self.close_wait.wait()
        self.closed = True

    def terminate(self) -> None:
        self.terminated = True
        self.closed = True
        self.close_wait.set()


class TestCloseBounded:
    """TaskQ.close() bounds the owned-pool close - a dead PG cannot wedge it."""

    async def test_close_bounds_hung_owned_pool_close(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An owned pool whose close() hangs (an enqueue in flight at close
        time against a dead PG): TaskQ.close() bounds the wait, terminates
        the pool, nulls ``_pool``, and returns instead of hanging forever.
        """
        import taskq.client._taskq as taskq_mod

        monkeypatch.setattr(taskq_mod, "CLOSE_TIMEOUT_SECS", 0.05)
        tq = TaskQ(dsn="postgresql://u:p@h:5432/db", schema=_SCHEMA_LABEL)
        fake_pool = _FakeHungClosePool()
        fake_pool.close_wait.clear()  # close() blocks forever from now on
        # Owned pool: dsn mode → _owns_pool is True, so close() must close it.
        tq._pool = cast(asyncpg.Pool, fake_pool)

        # Why the outer timeout: pre-fix close() awaits pool.close()
        # unbounded, so the RED state would hang forever instead of failing
        # fast.
        async with asyncio.timeout(5):
            await tq.close()

        assert fake_pool.close_calls == 1
        assert fake_pool.terminated is True
        assert tq._pool is None


# ---------------------------------------------------------------------------
# TestRedisUrlWiring - redis_url is coerced via settings, never stored raw
# ---------------------------------------------------------------------------


class TestRedisUrlWiring:
    """TaskQ(redis_url=...) routes the URL through TaskQSettings' RedisDsn
    field type instead of storing a raw str - and rejects empty URLs at
    construction rather than silently disabling Redis.
    """

    async def test_open_invalid_redis_url_raises_type_coercion_error(self) -> None:
        """open() with a non-Redis redis_url raises TypeCoercionError
        referencing redis_url - the field's RedisDsn type rejects the
        scheme at startup, not at first publish."""
        fake_pool = MagicMock(spec=asyncpg.Pool)
        tq = TaskQ(pool=fake_pool, redis_url="http://not-redis", schema=_SCHEMA_LABEL)
        with pytest.raises(TypeCoercionError, match="redis_url"):
            await tq.open()

    async def test_open_valid_redis_url_wires_redis_dsn_settings(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """After open() the wired settings carry a dotenvmodel RedisDsn
        instance (coerced from the str argument), which _open_redis
        receives - never a raw str."""
        from taskq.client._jobs import JobsClient
        from taskq.settings import TaskQSettings

        opened_with: list[TaskQSettings] = []

        async def _fake_open_redis(client: JobsClient, settings: TaskQSettings) -> None:
            # Stand-in for the real hook: records the wired settings and
            # skips the broker dial (client.initialize() would connect).
            opened_with.append(settings)

        monkeypatch.setattr(JobsClient, "_open_redis", _fake_open_redis)
        fake_pool = MagicMock(spec=asyncpg.Pool)
        tq = TaskQ(pool=fake_pool, redis_url="redis://localhost:6379/0", schema=_SCHEMA_LABEL)
        await tq.open()
        try:
            assert tq._client is not None
            redis_url = opened_with[0].redis_url
            assert isinstance(redis_url, RedisDsn)
            assert str(redis_url) == "redis://localhost:6379/0"
        finally:
            await tq.close()

    def test_constructor_blank_redis_url_raises_value_error(self) -> None:
        """redis_url="" (the os.getenv(..., "") anti-pattern) and a
        whitespace-only URL fail at construction - dotenvmodel would
        coerce "" to None and silently disable Redis otherwise."""
        fake_pool = MagicMock(spec=asyncpg.Pool)
        for blank in ("", "   "):
            with pytest.raises(ValueError, match="non-empty"):
                TaskQ(pool=fake_pool, redis_url=blank)


# ---------------------------------------------------------------------------
# TestEnqueue
# ---------------------------------------------------------------------------


class TestEnqueue:
    """TaskQ.enqueue public-behaviour tests."""

    async def test_enqueue_returns_job_handle(self, pg_dsn: str) -> None:
        """enqueue returns a JobHandle with a valid UUID job_id."""
        await _migrate(pg_dsn)
        async with TaskQ(dsn=pg_dsn, schema=_SCHEMA_LABEL) as tq:
            handle = await tq.enqueue(_test_actor, _Payload(value=1))

        assert isinstance(handle, JobHandle)
        assert isinstance(handle.job_id, UUID)

    async def test_enqueue_fresh_was_existing_false(self, pg_dsn: str) -> None:
        """A fresh enqueue has was_existing == False."""
        await _migrate(pg_dsn)
        async with TaskQ(dsn=pg_dsn, schema=_SCHEMA_LABEL) as tq:
            handle = await tq.enqueue(_test_actor, _Payload(value=2))

        assert handle.was_existing is False

    async def test_enqueue_idempotency_key_dedup_same_job_id(self, pg_dsn: str) -> None:
        """Two enqueues with the same idempotency_key return the same job_id."""
        await _migrate(pg_dsn)
        async with TaskQ(dsn=pg_dsn, schema=_SCHEMA_LABEL) as tq:
            handle1 = await tq.enqueue(
                _test_actor, _Payload(value=1), idempotency_key="tq-client-idem-1"
            )
            handle2 = await tq.enqueue(
                _test_actor, _Payload(value=9), idempotency_key="tq-client-idem-1"
            )

        assert handle1.job_id == handle2.job_id

    async def test_enqueue_idempotency_key_second_was_existing_true(self, pg_dsn: str) -> None:
        """The second enqueue with the same idempotency_key returns was_existing == True."""
        await _migrate(pg_dsn)
        async with TaskQ(dsn=pg_dsn, schema=_SCHEMA_LABEL) as tq:
            await tq.enqueue(_test_actor, _Payload(value=1), idempotency_key="tq-client-idem-2")
            handle2 = await tq.enqueue(
                _test_actor, _Payload(value=99), idempotency_key="tq-client-idem-2"
            )

        assert handle2.was_existing is True

    async def test_enqueue_idempotency_key_contention_raises_typed_error_not_bare_timeout(
        self, pg_dsn: str
    ) -> None:
        """A real ``TaskQ(dsn=...)`` client's enqueue with a contended
        idempotency key must raise the typed
        ``IdempotencyKeyLockTimeoutError``, not a bare ``builtins.TimeoutError``.

        The client pool ``TaskQ.open()`` builds arms a per-query
        ``command_timeout`` (``_CLIENT_POOL_COMMAND_TIMEOUT_SECS``, 10.0,
        ``client/_taskq.py``), and the idempotency arm's lock budget
        (``DEFAULT_IDEMPOTENCY_LOCK_TIMEOUT_MS``, 5000.0,
        ``backend/_enqueue.py``) is delivered clamped to a fixed share of
        that bound (``taskq.connections.bounded_lock_budget_ms``), so the
        savepoint's ``SET LOCAL lock_timeout`` fires first and the
        ``except LockNotAvailableError`` handler surfaces the typed error
        with its ``idempotency-lock-timeout`` warning log and
        ``idempotency_lock_timeout`` backpressure counter bump. On a REAL
        pool connection (unlike the fake-conn unit pins in
        ``test_lock_timeout_refusal_counters.py``, and unlike
        ``tests/test_rt_locks_actor_tx_enqueue_serialization.py``'s second
        connection, which deliberately opens with ``command_timeout=30.0``
        to sidestep the race the other way), an unclamped budget would be
        preempted by asyncpg's client-side timer, and the caller would
        only ever see an ``asyncio.CancelledError``-derived
        ``builtins.TimeoutError``.
        """
        await _migrate(pg_dsn)
        key = f"tq-client-contended-{new_base62()}".lower()

        # Holder: a raw connection with an open, uncommitted transaction
        # occupying the (idempotency_scope, idempotency_key) speculative
        # token row via the real enqueue path.
        holder_conn = await asyncpg.connect(pg_dsn)
        try:
            tr = holder_conn.transaction()
            await tr.start()
            try:
                # TaskQ.enqueue() has no connection= param, so the holder's
                # insert is driven directly via the client's underlying SQL
                # contract against this held-open transaction - the same
                # approach tests/test_rt_locks_actor_tx_enqueue_serialization.py
                # uses for its holder side.
                sql = render_sql(_SCHEMA_LABEL)
                await _enqueue_on_conn(
                    holder_conn,
                    sql,
                    _SCHEMA_LABEL,
                    SystemClock(),
                    make_enqueue_args(idempotency_key=key),
                )

                # Victim: the REAL client pool (5.0s command_timeout),
                # exactly as an application would use it.
                async with TaskQ(dsn=pg_dsn, schema=_SCHEMA_LABEL) as tq:
                    with pytest.raises(IdempotencyKeyLockTimeoutError) as excinfo:
                        await asyncio.wait_for(
                            tq.enqueue(_test_actor, _Payload(value=1), idempotency_key=key),
                            timeout=20.0,
                        )
                assert "idempotency_key" in repr(excinfo.value), (
                    "Contract: the client-visible enqueue must surface the typed "
                    f"IdempotencyKeyLockTimeoutError; got {excinfo.value!r}"
                )
            finally:
                await tr.rollback()
        finally:
            await holder_conn.close()

    async def test_enqueue_idempotency_key_contention_raises_typed_error_when_operator_widens_lock_timeout_above_default(
        self, pg_dsn: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An operator who raises ``TASKQ_IDEMPOTENCY_LOCK_TIMEOUT_MS`` above
        its shipped default must still see the typed
        ``IdempotencyKeyLockTimeoutError`` on contention, not a bare
        ``builtins.TimeoutError``.

        This pins the ordering claim in
        :func:`taskq.connections.lock_budget_command_timeout_secs`: a
        widened lock budget must re-derive the client pool's own
        ``command_timeout`` upward so the server-side ``lock_timeout``
        still fires first. If an operator could widen the lock-wait budget
        past the client's (fixed) network timeout, the exact same bare
        ``TimeoutError`` regression this issue reports would resurface -
        just at a different, operator-chosen threshold instead of the
        shipped default.
        """
        await _migrate(pg_dsn)
        key = f"tq-client-contended-wide-{new_base62()}".lower()

        # Operator widens the idempotency lock-wait budget well past its
        # 5000ms shipped default and past the client pool's shipped
        # command_timeout floor (10.0s) - the exact scenario the ordering
        # guarantee exists to cover.
        monkeypatch.setenv("TASKQ_IDEMPOTENCY_LOCK_TIMEOUT_MS", "15000")

        holder_conn = await asyncpg.connect(pg_dsn)
        try:
            tr = holder_conn.transaction()
            await tr.start()
            try:
                sql = render_sql(_SCHEMA_LABEL)
                await _enqueue_on_conn(
                    holder_conn,
                    sql,
                    _SCHEMA_LABEL,
                    SystemClock(),
                    make_enqueue_args(idempotency_key=key),
                )

                async with TaskQ(dsn=pg_dsn, schema=_SCHEMA_LABEL) as tq:
                    with pytest.raises(IdempotencyKeyLockTimeoutError) as excinfo:
                        await asyncio.wait_for(
                            tq.enqueue(_test_actor, _Payload(value=1), idempotency_key=key),
                            timeout=30.0,
                        )
                assert "idempotency_key" in repr(excinfo.value), (
                    "Contract: widening the operator lock-wait budget above its "
                    "default must not reintroduce a bare TimeoutError; got "
                    f"{excinfo.value!r}"
                )
            finally:
                await tr.rollback()
        finally:
            await holder_conn.close()

    async def test_enqueue_without_scheduled_at_status_is_pending(self, pg_dsn: str) -> None:
        """Enqueueing without scheduled_at results in a job with status='pending'."""
        await _migrate(pg_dsn)
        async with TaskQ(dsn=pg_dsn, schema=_SCHEMA_LABEL) as tq:
            handle = await tq.enqueue(_test_actor, _Payload(value=3))
            fetched = await tq.get(handle.job_id, result_adapter=_RA)

        assert fetched is not None
        assert fetched._row.status == "pending"

    async def test_enqueue_with_future_scheduled_at_stored_at_correct_time(
        self, pg_dsn: str
    ) -> None:
        """Enqueueing with a future scheduled_at stores the correct timestamp.

        The PG backend always inserts with status='pending' (no trigger flips
        it to 'scheduled' at insert time - that transition is done by the
        scheduled-to-pending sweep at dispatch). What we can assert is that the
        stored scheduled_at matches the value we supplied.
        """
        await _migrate(pg_dsn)
        future = datetime.now(UTC) + timedelta(hours=1)
        async with TaskQ(dsn=pg_dsn, schema=_SCHEMA_LABEL) as tq:
            handle = await tq.enqueue(_test_actor, _Payload(value=4), scheduled_at=future)
            fetched = await tq.get(handle.job_id, result_adapter=_RA)

        assert fetched is not None
        # scheduled_at is stored faithfully (within sub-second PG rounding).
        stored_at = fetched._row.scheduled_at
        assert stored_at is not None
        diff = abs((stored_at - future).total_seconds())
        assert diff < 1.0


# ---------------------------------------------------------------------------
# TestGet
# ---------------------------------------------------------------------------


class TestGet:
    """TaskQ.get public-behaviour tests."""

    async def test_get_existing_job_returns_handle(self, pg_dsn: str) -> None:
        """get(job_id) returns a JobHandle for an existing job."""
        await _migrate(pg_dsn)
        async with TaskQ(dsn=pg_dsn, schema=_SCHEMA_LABEL) as tq:
            enqueued = await tq.enqueue(_test_actor, _Payload(value=10))
            found = await tq.get(enqueued.job_id, result_adapter=_RA)

        assert found is not None
        assert isinstance(found, JobHandle)
        assert found.job_id == enqueued.job_id

    async def test_get_unknown_id_returns_none(self, pg_dsn: str) -> None:
        """get(unknown_id) returns None - does not raise."""
        await _migrate(pg_dsn)
        async with TaskQ(dsn=pg_dsn, schema=_SCHEMA_LABEL) as tq:
            result = await tq.get(new_job_id(), result_adapter=_RA)

        assert result is None


# ---------------------------------------------------------------------------
# TestGetRow
# ---------------------------------------------------------------------------


class TestGetRow:
    """TaskQ.get_row public-behaviour tests - raw JobRow, no handle."""

    async def test_get_row_existing_job_returns_row(self, pg_dsn: str) -> None:
        """get_row(job_id) returns the raw JobRow for an existing job."""
        await _migrate(pg_dsn)
        async with TaskQ(dsn=pg_dsn, schema=_SCHEMA_LABEL) as tq:
            enqueued = await tq.enqueue(_test_actor, _Payload(value=10))
            row = await tq.get_row(enqueued.job_id)

        assert row is not None
        assert isinstance(row, JobRow)
        assert row.id == enqueued.job_id
        assert row.actor == "tq_client_test_actor"
        assert row.payload == {"value": 10}

    async def test_get_row_unknown_id_returns_none(self, pg_dsn: str) -> None:
        """get_row(unknown_id) returns None - does not raise."""
        await _migrate(pg_dsn)
        async with TaskQ(dsn=pg_dsn, schema=_SCHEMA_LABEL) as tq:
            result = await tq.get_row(new_job_id())

        assert result is None


# ---------------------------------------------------------------------------
# TestList
# ---------------------------------------------------------------------------


class TestList:
    """TaskQ.list filtering tests."""

    async def test_list_by_queue_returns_enqueued_job(self, pg_dsn: str) -> None:
        """list(JobFilter(queue='default')) returns a page containing the enqueued job."""
        await _migrate(pg_dsn)
        async with TaskQ(dsn=pg_dsn, schema=_SCHEMA_LABEL) as tq:
            handle = await tq.enqueue(_test_actor, _Payload(value=20))
            page = await tq.list(JobFilter(queue="default"))

        assert any(j.id == handle.job_id for j in page.jobs)

    async def test_list_by_status_pending_filters_correctly(self, pg_dsn: str) -> None:
        """list(JobFilter(status='pending')) returns only pending jobs."""
        await _migrate(pg_dsn)
        async with TaskQ(dsn=pg_dsn, schema=_SCHEMA_LABEL) as tq:
            handle = await tq.enqueue(_test_actor, _Payload(value=21))
            page = await tq.list(JobFilter(status="pending"))

        assert any(j.id == handle.job_id for j in page.jobs)
        assert all(j.status == "pending" for j in page.jobs)

    async def test_list_by_nonexistent_actor_returns_empty_page(self, pg_dsn: str) -> None:
        """list(JobFilter(actor='no_such_actor')) returns an empty page."""
        await _migrate(pg_dsn)
        async with TaskQ(dsn=pg_dsn, schema=_SCHEMA_LABEL) as tq:
            # Ensure at least one job exists so the filter is meaningful.
            await tq.enqueue(_test_actor, _Payload(value=22))
            page = await tq.list(JobFilter(actor="no_such_actor"))

        assert page.jobs == []


# ---------------------------------------------------------------------------
# TestCancel
# ---------------------------------------------------------------------------


class TestCancel:
    """TaskQ.cancel public-behaviour tests."""

    async def test_cancel_pending_job_returns_cancel_result(self, pg_dsn: str) -> None:
        """cancel(job_id) on a pending job returns CancelResult with
        cancellation_initiated=True and previous_status='pending'.
        """
        await _migrate(pg_dsn)
        async with TaskQ(dsn=pg_dsn, schema=_SCHEMA_LABEL) as tq:
            handle = await tq.enqueue(_test_actor, _Payload(value=30))
            result = await tq.cancel(handle.job_id)

        assert isinstance(result, CancelResult)
        assert result.cancellation_initiated is True
        assert result.previous_status == "pending"
        assert result.job_id == handle.job_id

    async def test_cancel_unknown_id_raises_key_error(self, pg_dsn: str) -> None:
        """cancel(unknown_id) raises KeyError."""
        await _migrate(pg_dsn)
        missing_id: JobId = new_job_id()
        async with TaskQ(dsn=pg_dsn, schema=_SCHEMA_LABEL) as tq:
            with pytest.raises(KeyError):
                await tq.cancel(missing_id)


# ---------------------------------------------------------------------------
# TestSchedules
# ---------------------------------------------------------------------------


class TestSchedules:
    """TaskQ.update_schedule / TaskQ.delete_schedule delegation tests."""

    async def test_update_schedule_delegates_and_returns_updated_record(self, pg_dsn: str) -> None:
        """update_schedule() delegates to JobsClient.update_schedule and the
        returned ScheduleRecord reflects the requested change.
        """
        await _migrate(pg_dsn)
        async with TaskQ(dsn=pg_dsn, schema=_SCHEMA_LABEL) as tq:
            created = await tq.create_schedule(
                _test_actor,
                "0 * * * *",
                name="tq-client-update-schedule",
                enabled=True,
            )

            updated = await tq.update_schedule(created.schedule_id, enabled=False)

            assert updated.id == created.schedule_id
            assert updated.enabled is False

            listed = await tq.list_schedules()

        matching = [s for s in listed if s.id == created.schedule_id]
        assert len(matching) == 1
        assert matching[0].enabled is False

    async def test_delete_schedule_delegates_and_removes_schedule(self, pg_dsn: str) -> None:
        """delete_schedule() delegates to JobsClient.delete_schedule; the
        schedule no longer appears in list_schedules() afterwards.
        """
        await _migrate(pg_dsn)
        async with TaskQ(dsn=pg_dsn, schema=_SCHEMA_LABEL) as tq:
            created = await tq.create_schedule(
                _test_actor,
                "0 * * * *",
                name="tq-client-delete-schedule",
                enabled=True,
            )

            await tq.delete_schedule(created.schedule_id)

            listed = await tq.list_schedules()

        assert all(s.id != created.schedule_id for s in listed)

    async def test_delete_schedule_is_idempotent(self, pg_dsn: str) -> None:
        """delete_schedule() on an already-deleted schedule does not raise
        (delegation is idempotent per JobsClient contract).
        """
        await _migrate(pg_dsn)
        async with TaskQ(dsn=pg_dsn, schema=_SCHEMA_LABEL) as tq:
            created = await tq.create_schedule(
                _test_actor,
                "0 * * * *",
                name="tq-client-delete-schedule-twice",
                enabled=True,
            )
            await tq.delete_schedule(created.schedule_id)
            await tq.delete_schedule(created.schedule_id)  # must not raise


# ---------------------------------------------------------------------------
# TestStream
# ---------------------------------------------------------------------------


class TestStream:
    """TaskQ.stream behaviour tests."""

    async def test_stream_on_terminal_job_yields_one_event(self, pg_dsn: str) -> None:
        """stream() on a job that is already terminal yields exactly one
        JobEvent with terminal=True and returns.
        """
        await _migrate(pg_dsn)
        async with TaskQ(dsn=pg_dsn, schema=_SCHEMA_LABEL) as tq:
            handle = await tq.enqueue(_test_actor, _Payload(value=40))
            await tq.cancel(handle.job_id)
            events: list[JobEvent] = []
            async for event in tq.stream(handle.job_id):
                events.append(event)

        assert len(events) == 1
        assert events[0].terminal is True
        assert events[0].status == "cancelled"

    async def test_stream_on_nonexistent_job_raises_key_error(self, pg_dsn: str) -> None:
        """stream() on a non-existent job_id raises KeyError."""
        await _migrate(pg_dsn)
        async with TaskQ(dsn=pg_dsn, schema=_SCHEMA_LABEL) as tq:
            with pytest.raises(KeyError):
                async for _ in tq.stream(new_job_id()):
                    pass

    async def test_stream_before_open_raises_runtime_error(self, pg_dsn: str) -> None:
        """stream() called outside async with block raises RuntimeError."""
        tq = TaskQ(dsn=pg_dsn, schema=_SCHEMA_LABEL)
        with pytest.raises(RuntimeError, match=r"tq\.open"):
            async for _ in tq.stream(new_job_id()):
                pass


# ---------------------------------------------------------------------------
# TestStreamPgInternals - direct exercise of the PG LISTEN/NOTIFY transport
# ---------------------------------------------------------------------------


async def _set_job_status(pool: asyncpg.Pool, schema: str, job_id: UUID, status: str) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            f'UPDATE "{schema}".jobs SET status = $1 WHERE id = $2',  # noqa: S608 - schema is a worker-scoped constant, not user input; values are $N-bound
            status,
            job_id,
        )


async def _delete_job(pool: asyncpg.Pool, schema: str, job_id: UUID) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            f'DELETE FROM "{schema}".jobs WHERE id = $1',  # noqa: S608 - schema is a worker-scoped constant, not user input; values are $N-bound
            job_id,
        )


class TestStreamPgInternals:
    """Direct unit tests for :func:`taskq.client._taskq._stream_pg`.

    Bypasses ``TaskQ.stream()`` / a live worker to drive multi-event
    transitions deterministically. Uses a small ``poll_timeout`` so the
    poll loop advances quickly.
    """

    async def test_stream_pg_yields_on_change_and_returns_on_terminal(self, pg_dsn: str) -> None:
        """_stream_pg yields an event for each detected status change and
        returns once a terminal status is observed.
        """
        await _migrate(pg_dsn)
        async with TaskQ(dsn=pg_dsn, schema=_SCHEMA_LABEL) as tq:
            handle = await tq.enqueue(_test_actor, _Payload(value=100))
            client = tq._client
            assert client is not None
            pool = tq._pool
            assert pool is not None

            events: list[JobEvent] = []

            async def _consume() -> None:
                async for evt in _stream_pg(
                    handle.job_id,
                    client,
                    0.05,
                    last_seq=-1,
                    last_status=None,
                ):
                    events.append(evt)

            consumer = asyncio.create_task(_consume())
            await asyncio.sleep(0.2)
            await _set_job_status(pool, _SCHEMA_LABEL, handle.job_id, "running")
            await asyncio.sleep(0.2)
            await _set_job_status(pool, _SCHEMA_LABEL, handle.job_id, "succeeded")
            await asyncio.wait_for(consumer, timeout=5)

        statuses = [e.status for e in events]
        assert "running" in statuses
        assert statuses[-1] == "succeeded"
        assert events[-1].terminal is True

    async def test_stream_via_taskq_multiple_pg_events(self, pg_dsn: str) -> None:
        """TaskQ.stream() (PG transport, no redis_client) yields more than one
        JobEvent across successive status transitions before terminating -
        exercises the ``_stream_pg`` delegation branch in ``TaskQ.stream()``.
        """
        await _migrate(pg_dsn)
        async with TaskQ(dsn=pg_dsn, schema=_SCHEMA_LABEL, poll_timeout=0.05) as tq:
            handle = await tq.enqueue(_test_actor, _Payload(value=104))
            pool = tq._pool
            assert pool is not None

            events: list[JobEvent] = []

            async def _consume() -> None:
                async for evt in tq.stream(handle.job_id):
                    events.append(evt)

            consumer = asyncio.create_task(_consume())
            await asyncio.sleep(0.2)
            await _set_job_status(pool, _SCHEMA_LABEL, handle.job_id, "running")
            await asyncio.sleep(0.2)
            await _set_job_status(pool, _SCHEMA_LABEL, handle.job_id, "succeeded")
            await asyncio.wait_for(consumer, timeout=5)

        assert len(events) >= 2
        assert events[-1].terminal is True
        assert events[-1].status == "succeeded"

    async def test_stream_pg_raises_key_error_when_job_disappears(self, pg_dsn: str) -> None:
        """_stream_pg raises KeyError if the job row disappears mid-stream."""
        await _migrate(pg_dsn)
        async with TaskQ(dsn=pg_dsn, schema=_SCHEMA_LABEL) as tq:
            handle = await tq.enqueue(_test_actor, _Payload(value=101))
            client = tq._client
            assert client is not None
            pool = tq._pool
            assert pool is not None

            async def _consume() -> None:
                async for _ in _stream_pg(
                    handle.job_id,
                    client,
                    0.05,
                    last_seq=-1,
                    last_status=None,
                ):
                    pass

            consumer = asyncio.create_task(_consume())
            await asyncio.sleep(0.2)
            await _delete_job(pool, _SCHEMA_LABEL, handle.job_id)

            with pytest.raises(KeyError):
                await asyncio.wait_for(consumer, timeout=5)


# ---------------------------------------------------------------------------
# TestStreamRedisInternals - direct unit tests for _stream_redis
# ---------------------------------------------------------------------------


def _stub_backend_sequence(rows: list[JobRow | None]) -> Backend:
    """Build a stub Backend where ``get`` returns successive values from *rows*."""
    remaining = list(rows)
    backend = AsyncMock(spec=Backend)

    async def _get(job_id: JobId) -> JobRow | None:
        if remaining:
            return remaining.pop(0)
        return None

    backend.get = _get
    return backend


class TestStreamRedisInternals:
    """Direct unit tests for :func:`taskq.client._taskq._stream_redis`."""

    async def test_stream_redis_refetch_raises_key_error_when_job_disappears(
        self, pg_dsn: str
    ) -> None:
        """_refetch() raises KeyError when the job row disappears between the
        initial fetch and a later redis-triggered refetch.
        """
        from taskq.client._jobs import JobsClient
        from taskq.progress._events import ProgressEvent
        from taskq.settings import TaskQSettings

        job_id = new_job_id()
        backend = _stub_backend_sequence([None])
        settings_schema = _SCHEMA_LABEL

        settings = TaskQSettings.load_from_dict({"TASKQ_SCHEMA_NAME": settings_schema})
        client = JobsClient(backend, settings=settings)

        pubsub = AsyncMock()
        pubsub.subscribe = AsyncMock()
        pubsub.unsubscribe = AsyncMock()
        pubsub.aclose = AsyncMock()

        progress_event = ProgressEvent(
            kind="progress",
            job_id=job_id,
            actor="test_actor",
            ts=datetime.now(UTC),
            seq=1,
            status="running",
        )
        raw_data = progress_event.model_dump_json(exclude_none=True).encode("utf-8")

        message_calls = 0

        async def _get_message(
            *,
            ignore_subscribe_messages: bool = True,
            timeout: float = 0,  # noqa: ASYNC109 - mirrors redis-py's get_message signature to monkeypatch it in a test
        ) -> dict[str, object] | None:
            nonlocal message_calls
            message_calls += 1
            if message_calls == 1:
                return {"type": "message", "data": raw_data}
            return None

        pubsub.get_message = _get_message
        redis_client = MagicMock(spec=["pubsub"])
        redis_client.pubsub.return_value = pubsub

        with pytest.raises(KeyError):
            async for _ in _stream_redis(
                redis_client,
                settings_schema,
                job_id,
                client,
                30.0,
                last_seq=-1,
                last_status=None,
            ):
                pass

"""Regression guards for four verified "silent failure" defects.

Each test asserts the DESIRABLE behaviour, so it goes GREEN once the
corresponding defect is fixed and RED on ``16c991d``.

1. ``heartbeat_timeout`` is accepted on the public enqueue API, stored on
   ``EnqueueArgs``/``JobRow``, written to PG and hydrated back, but is
   read by nothing under ``src/taskq/worker/`` — no dispatch path, no
   sweep path, no validation, no warning.  The only enforced sibling is
   the GLOBAL ``lock_lease``.
2. ``taskq.cli._load_actor_registry`` type-guards an iterable with
   ``all(isinstance(r, ActorRef) for r in raw)``, which CONSUMES a
   one-shot iterator, then rebuilds ``{r.name: r for r in raw}`` from the
   exhausted object and returns ``{}``.  ``all()`` over an empty iterable
   is vacuously True, so the guard cannot detect its own failure.
3. ``enqueue_select_by_key`` has no status predicate and no time window,
   so an idempotency key whose job failed weeks ago still dedupes.  The
   dedup log line omits the target's ``status`` and logs at ``info``.
4. Three failure paths log and never count: reservation/rate-limit
   denial, sub-enqueue flush failure, and progress FLUSH failure — while
   structurally identical siblings (``taskq.ratelimit.refund_failures``,
   ``taskq.progress.publish_failures``) do have counters.  Both
   progress-flush handlers additionally emit the identical event name
   for two materially different failures.
"""

import asyncio
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import asyncpg
import pytest
import structlog
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from pydantic import BaseModel, TypeAdapter
from typer.testing import CliRunner

import taskq.obs as obs_mod
import taskq.obs._otel as otel_mod
from taskq._ids import new_job_id, new_uuid
from taskq.actor import ActorRef
from taskq.cli import _load_actor_registry, app
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.exceptions import ReservationUnavailable
from taskq.progress._buffer import _ProgressBuffer
from taskq.progress._flush import _flush_buffer, progress_flush_loop
from taskq.ratelimit._decision_log import log_decision
from taskq.ratelimit.decision import RateLimitDecision
from taskq.retry import RetryPolicy
from taskq.testing.clock import FakeClock
from taskq.testing.fixtures import ModulePgSchema, _open_pg_backend_on_schema
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.jobs import make_enqueue_args
from taskq.testing.otel import counter_data_points, counter_value

runner = CliRunner()

_NOW = datetime(2025, 1, 1, tzinfo=UTC)


class _Payload(BaseModel):
    value: str = "test"


class _Result(BaseModel):
    ok: bool = True


def _make_actor_ref(name: str = "child") -> ActorRef[_Payload, _Result]:
    async def _handler(payload: _Payload) -> _Result:
        return _Result()

    return ActorRef(
        name=name,
        queue="default",
        fn=_handler,
        wants_ctx=False,
        dependencies={},
        payload_type=_Payload,
        result_adapter=TypeAdapter(_Result),
        retry=RetryPolicy(),
        result_ttl=None,
        singleton=False,
        unique_for=None,
        max_pending=None,
    )


def _make_enqueuer(backend: InMemoryBackend) -> SubJobEnqueuer:
    return SubJobEnqueuer(
        loop_scope_resolved=None,
        worker_pool=object(),
        backend=backend,
        clock=FakeClock(_NOW),
    )


@pytest.fixture
def otel_reader(monkeypatch: pytest.MonkeyPatch) -> InMemoryMetricReader:
    """Per-test OTel meter isolation.

    Patches ``obs._otel.get_meter`` onto a fresh MeterProvider backed by an
    ``InMemoryMetricReader``, so instruments created lazily by the code
    under test land in this reader.  Mirrors ``tests/test_obs.py``.
    """
    from opentelemetry.sdk.metrics import MeterProvider

    reader = InMemoryMetricReader()
    new_provider = MeterProvider(metric_readers=[reader])
    new_meter = new_provider.get_meter(obs_mod.INSTRUMENTATION_NAME, otel_mod._version())  # pyright: ignore[reportPrivateUsage]  # Why: mirrors tests/test_obs.py's otel_reader fixture, which reads the same private version helper.

    monkeypatch.setattr(otel_mod, "get_meter", lambda: new_meter)
    monkeypatch.setattr(obs_mod, "get_meter", lambda: new_meter)
    otel_mod.set_otel_enabled(True)
    return reader


# ── DEFECT 1: per-job heartbeat_timeout is accepted and discarded ─────────


class TestHeartbeatTimeoutIsNotSilentlyDiscarded:
    """Contract chosen: **enforced** (direction (a) — the reclamation
    contract the original refusal docstring named as the alternative).

    ``heartbeat_timeout`` was once enforced NOWHERE — accepted on the
    public API, stored on the row, and read by nothing — and the interim
    contract this suite pinned was refusal at the enqueue boundary. The
    project has since chosen the other acceptable contract: the leader's
    reclaim sweep reclaims a running job whose holder has been silent
    past its per-job ``heartbeat_timeout`` (pinned against PG and the
    in-memory twin in ``tests/test_heartbeat_timeout_enforced.py``, the
    reclamation test this suite's original docstring specified as the
    replacement).

    What must never return is the SILENT discard: a caller that sets a
    10-second heartbeat timeout and gets the 60-second global lease
    instead has no way to learn that from the library. Under direction
    (a) that guard is "the value reaches the stored row the sweep
    reads" — an enqueue that drops it would silently revert the caller
    to lease-only reclamation with no signal.
    """

    async def test_enqueue_with_heartbeat_timeout_is_carried_to_the_row(self) -> None:
        """The value must reach the stored row — the row the reclaim
        sweep's heartbeat arm reads. An enqueue that dropped it would
        silently revert the caller to global-lease-only reclamation."""
        backend = InMemoryBackend(clock=FakeClock(_NOW))
        enqueuer = _make_enqueuer(backend)

        handle = await enqueuer.enqueue(
            _make_actor_ref(),
            _Payload(),
            heartbeat_timeout=timedelta(seconds=10),
        )
        row = await backend.get(handle.job_id)
        assert row is not None, "enqueue returned a handle for a row the backend cannot read"
        assert row.heartbeat_timeout == timedelta(seconds=10), (
            f"heartbeat_timeout was silently discarded on the way to the row "
            f"(got {row.heartbeat_timeout!r}) — the reclaim sweep reads the "
            "stored column, so the caller's per-job liveness promise would "
            "silently no-op."
        )

    async def test_enqueue_without_heartbeat_timeout_still_works(self) -> None:
        """The parameter is opt-in: no value, no per-job deadline."""
        backend = InMemoryBackend(clock=FakeClock(_NOW))
        enqueuer = _make_enqueuer(backend)

        handle = await enqueuer.enqueue(_make_actor_ref(), _Payload())
        row = await backend.get(handle.job_id)
        assert row is not None
        assert row.heartbeat_timeout is None


# ── DEFECT 2: CLI builds an EMPTY registry from a one-shot iterator ───────


_ITER_ACTOR_A = _make_actor_ref("iter_actor_a")
_ITER_ACTOR_B = _make_actor_ref("iter_actor_b")

_MODULE = "tests.test_silent_failure_guards"
_EMPTY_LIST_PATH = f"{_MODULE}:_EMPTY_ACTOR_LIST"

_EMPTY_ACTOR_LIST: list[ActorRef[Any, Any]] = []


def _actor_generator() -> Iterator[ActorRef[Any, Any]]:
    """A one-shot generator of ActorRefs — the shape ``Iterable[ActorRef]`` documents."""
    yield _ITER_ACTOR_A
    yield _ITER_ACTOR_B


def _identity_actor(ref: ActorRef[Any, Any]) -> ActorRef[Any, Any]:
    """Identity, so ``map`` below is a genuine one-shot iterator over ActorRefs."""
    return ref


def _install_one_shot(monkeypatch: pytest.MonkeyPatch, name: str, value: object) -> str:
    """Publish a fresh one-shot iterator on this module and return its ``module:attr`` ref.

    A one-shot iterator is consumed by the very defect under test, so each
    test needs its own rather than sharing a module-level singleton that a
    sibling test already drained.
    """
    import tests.test_silent_failure_guards as _self

    monkeypatch.setattr(_self, name, value, raising=False)
    return f"{_MODULE}:{name}"


class TestActorRegistryFromOneShotIterator:
    """``_load_actor_registry`` must not silently return ``{}``.

    The ``all(isinstance(...))`` type guard consumes the iterator; the
    dict comprehension then rebuilds from an exhausted object.  Nothing
    downstream distinguishes ``{}`` from a populated mapping (every
    consumer checks ``is not None``), so the worker boots and dispatches
    nothing.
    """

    def test_generator_of_actor_refs_yields_populated_registry(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A generator (the documented Iterable[ActorRef] shape) must survive the type guard."""
        ref = _install_one_shot(monkeypatch, "_gen_actors", _actor_generator())
        registry = _load_actor_registry(ref)
        assert dict(registry) == {
            "iter_actor_a": _ITER_ACTOR_A,
            "iter_actor_b": _ITER_ACTOR_B,
        }

    def test_map_object_yields_populated_registry(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The defect is not generator-specific: ``map`` is one-shot too."""
        ref = _install_one_shot(
            monkeypatch,
            "_map_actors",
            map(_identity_actor, (_ITER_ACTOR_A, _ITER_ACTOR_B)),
        )
        registry = _load_actor_registry(ref)
        assert set(registry) == {"iter_actor_a", "iter_actor_b"}

    def test_filter_object_yields_populated_registry(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """``filter`` is one-shot as well."""
        ref = _install_one_shot(
            monkeypatch, "_filter_actors", filter(None, (_ITER_ACTOR_A, _ITER_ACTOR_B))
        )
        registry = _load_actor_registry(ref)
        assert set(registry) == {"iter_actor_a", "iter_actor_b"}

    def test_generator_registry_reaches_worker_main_populated(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """End-to-end through the CLI: worker_main must not receive an empty registry."""
        captured: dict[str, Any] = {}

        def fake_worker_main(settings: Any, *, actor_registry: Any = None, **kwargs: Any) -> int:
            captured["registry"] = actor_registry
            return 0

        monkeypatch.setattr("taskq.cli._worker_main", fake_worker_main)
        ref = _install_one_shot(monkeypatch, "_cli_gen_actors", _actor_generator())
        result = runner.invoke(app, ["worker", "--actors", ref])
        assert result.exit_code == 0, f"stderr: {result.stderr}"
        assert captured["registry"] is not None
        assert set(captured["registry"]) == {"iter_actor_a", "iter_actor_b"}

    def test_empty_iterable_is_rejected_before_boot(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """An empty registry cannot dispatch anything — refuse it rather than boot idle.

        ``_worker_main`` is stubbed to a success return so the only way this
        test can exit non-zero is the registry check itself.
        """

        def fake_worker_main(settings: Any, *, actor_registry: Any = None, **kwargs: Any) -> int:
            return 0

        monkeypatch.setattr("taskq.cli._worker_main", fake_worker_main)
        result = runner.invoke(app, ["worker", "--actors", _EMPTY_LIST_PATH])
        assert result.exit_code == 1, f"stderr: {result.stderr}"
        assert "empty" in result.stderr.lower()


# ── DEFECT 3: enqueue dedup onto a TERMINAL job is silent ─────────────────


class TestTerminalDedupIsObservable:
    """Dedup onto a terminal (failed/succeeded/cancelled) job must be loud.

    ``enqueue_select_by_key`` (``_sql_templates.py:855``) is
    ``SELECT * FROM jobs WHERE idempotency_scope = $1 AND idempotency_key = $2``
    — no status predicate, no time window — so a key whose job failed
    weeks ago still dedupes, bounded only by ``DEFAULT_PRUNE_RETENTION``
    (30 days).  The library never distinguishes a terminal target from a
    live one at the point of return: the dedup log line is ``info`` and
    carries no ``status`` field.

    Scope note: ``JobHandle.was_existing`` and ``handle.row.status`` DO
    exist, so a caller *can* detect this today.  These tests therefore
    assert the missing **observable** (a warning carrying the target's
    status), not a new handle field.  ``unique_for`` is NOT affected —
    ``unique_states`` defaults to ``("pending", "scheduled", "running")``
    — and is deliberately not asserted on here.
    """

    pytestmark = pytest.mark.integration

    async def test_dedup_onto_failed_job_warns_with_status(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """Re-enqueuing a key whose job is terminal must warn and name the status."""
        stack, _deps, backend = await _open_pg_backend_on_schema(
            module_pg_schema.pg_dsn,
            module_pg_schema.schema_name,
        )
        try:
            first = await backend.enqueue(make_enqueue_args(idempotency_key="terminal-dedup-1"))
            await clean_pg_conn.execute(
                f'UPDATE "{module_pg_schema.schema_name}".jobs '  # noqa: S608  # Why: schema name comes from the test fixture, not user input.
                "SET status = 'failed', finished_at = now() WHERE id = $1::uuid",
                first.id,
            )

            with structlog.testing.capture_logs() as captured:
                second = await backend.enqueue(
                    make_enqueue_args(idempotency_key="terminal-dedup-1")
                )

            assert second.id == first.id, "precondition: the terminal row still dedupes"

            dedup_lines = [e for e in captured if e.get("event") == "enqueue_deduplicated"]
            assert dedup_lines, f"no dedup log line emitted; captured={captured}"
            line = dedup_lines[0]
            assert line.get("status") == "failed", (
                f"dedup onto a terminal job must record the target's status; got {line!r}"
            )
            assert line.get("log_level") == "warning", (
                "dedup onto a terminal job must be louder than a live-job hit; "
                f"got log_level={line.get('log_level')!r}"
            )
        finally:
            await stack.aclose()

    async def test_dedup_onto_live_job_stays_info(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """Dedup onto a still-pending job is normal operation and must stay at info."""
        stack, _deps, backend = await _open_pg_backend_on_schema(
            module_pg_schema.pg_dsn,
            module_pg_schema.schema_name,
        )
        try:
            first = await backend.enqueue(make_enqueue_args(idempotency_key="live-dedup-1"))

            with structlog.testing.capture_logs() as captured:
                second = await backend.enqueue(make_enqueue_args(idempotency_key="live-dedup-1"))

            assert second.id == first.id
            dedup_lines = [e for e in captured if e.get("event") == "enqueue_deduplicated"]
            assert dedup_lines, f"no dedup log line emitted; captured={captured}"
            line = dedup_lines[0]
            assert line.get("log_level") == "info"
            assert line.get("status") == "pending", (
                "the dedup line must carry the target's status on every hit, "
                f"not only terminal ones; got {line!r}"
            )
        finally:
            await stack.aclose()


# ── DEFECT 4: failure paths that log but never count ──────────────────────


class TestReservationDenialIsCounted:
    """Rate-limit / reservation denial logs and emits no metric.

    ``grep -rn "denial\\|denied" src/taskq/obs/`` finds only an unrelated
    docstring — there is no denial counter at all, while the adjacent
    refund-failure path has ``taskq.ratelimit.refund_failures``.
    """

    def test_rate_limit_denial_increments_counter(self, otel_reader: InMemoryMetricReader) -> None:
        """A denied rate-limit decision must bump a denial counter, not just log."""
        denied = RateLimitDecision(
            bucket_name="my_bucket",
            backend="redis",
            allowed=False,
            remaining=0.0,
            retry_after=timedelta(seconds=1),
        )
        log_decision(denied)

        assert counter_value(otel_reader, "taskq.ratelimit.denials") == 1

    def test_allowed_decision_does_not_increment_counter(
        self, otel_reader: InMemoryMetricReader
    ) -> None:
        """A granted decision must not be counted as a denial."""
        allowed = RateLimitDecision(
            bucket_name="my_bucket",
            backend="redis",
            allowed=True,
            remaining=5.0,
            retry_after=None,
        )
        log_decision(allowed)

        assert counter_value(otel_reader, "taskq.ratelimit.denials") == 0

    def test_reservation_unavailable_is_counted(self, otel_reader: InMemoryMetricReader) -> None:
        """The reservation-denial arm needs its own observable too."""
        obs_record = getattr(obs_mod, "record_reservation_denial", None)
        assert obs_record is not None, (
            "taskq.obs exposes no record_reservation_denial — the "
            "ReservationUnavailable raise sites log only"
        )
        exc = ReservationUnavailable(
            bucket_name="my_reservation",
            retry_after=timedelta(seconds=1),
            source="reservation",
        )
        obs_record(exc.bucket_name, "reservation")

        assert counter_value(otel_reader, "taskq.reservation.denials") == 1


class TestSubEnqueueFlushFailureIsCounted:
    """``sub_enqueue_flush_failed`` logs at error and emits no metric.

    ``grep -rin "sub_enqueue" src/taskq/obs/`` returns nothing, while the
    structurally identical refund-failure sibling has a counter
    (``_otel.py:730``).
    """

    def test_obs_exposes_a_sub_enqueue_failure_recorder(
        self, otel_reader: InMemoryMetricReader
    ) -> None:
        """A sub-enqueue flush failure must be countable, not only loggable."""
        record = getattr(obs_mod, "record_sub_enqueue_failure", None)
        assert record is not None, (
            "taskq.obs exposes no record_sub_enqueue_failure — the "
            "sub_enqueue_flush_failed catch site in worker/_consumer.py logs only"
        )
        record("test_actor", 2)

        assert counter_value(otel_reader, "taskq.sub_enqueue.failures") == 2


def _dirty_buffer(job_id: UUID, *, state: dict[str, object] | None = None) -> _ProgressBuffer:
    """A dirty progress buffer with one pending seq delta."""
    return _ProgressBuffer(
        job_id=job_id,
        base_seq=0,
        pending_seq_delta=1,
        pending_state=state if state is not None else {},
        dirty=True,
    )


class _FailingPool:
    """A pool whose ``acquire`` raises — a pool-wide outage: every job's
    flush that tick is lost, not just one job's."""

    def acquire(self) -> Any:
        raise asyncpg.PostgresConnectionError("pool acquire failed")


class _StatementFailingPool:
    """A pool that acquires fine but hands out a connection whose
    ``fetchrow`` raises — the per-job flush failure path (the UPDATE
    itself fails; only this job's delta is lost)."""

    def acquire(self) -> "_FailingConnCtx":
        return _FailingConnCtx()


class _FailingConnCtx:
    async def __aenter__(self) -> "_FailingConn":
        return _FailingConn()

    async def __aexit__(self, *args: object) -> None:
        pass


class _FailingConn:
    async def fetchrow(self, *args: object) -> Any:
        raise asyncpg.PostgresError("simulated flush UPDATE failure")


class TestProgressFlushFailureIsCounted:
    """Progress FLUSH failures log and count, with the two materially
    different incidents distinguishable in both the metric stage label
    and the log kind.

    A per-job flush failure (the UPDATE statement itself fails — one
    job's progress delta is lost) is labeled ``stage='per_job'`` /
    ``kind="progress_flush_error"``. A pool-stage failure — the flush
    loop cannot obtain a pool at all, or the per-job acquire fails —
    loses EVERY job's progress delta and is labeled ``stage='pool'`` /
    ``kind="progress_flush_pool_error"``. The two must stay
    distinguishable in an alert rule, which is why the acquire failure
    and the statement failure sit in separate handlers rather than one
    catch-all around the whole acquire-then-update span.
    """

    async def test_per_job_flush_failure_increments_counter(
        self, otel_reader: InMemoryMetricReader
    ) -> None:
        """A failed UPDATE for one job must bump a flush-failure counter."""
        job_id = UUID(str(new_job_id()))
        buffer = _dirty_buffer(job_id, state={"step": "one"})
        buffers: dict[UUID, _ProgressBuffer] = {job_id: buffer}

        await _flush_buffer(
            _StatementFailingPool(),  # type: ignore[arg-type]  # Why: a minimal stand-in for asyncpg.Pool; only acquire() and the conn's fetchrow are reached.
            "taskq",
            job_id,
            new_uuid(),
            buffer,
            buffers,
        )

        assert counter_value(otel_reader, "taskq.progress.flush_failures") == 1
        dps = counter_data_points(otel_reader, "taskq.progress.flush_failures")
        assert dps and dps[0].attributes == {
            "stage": "per_job",
            "error_type": "PostgresError",
        }, f"per-job statement failure must carry stage='per_job': {dps}"

    async def test_pool_acquire_failure_records_the_pool_stage(
        self, otel_reader: InMemoryMetricReader
    ) -> None:
        """A pool-wide acquire failure loses every job's flush that tick —
        it must be counted at ``stage='pool'`` with the pool log kind, not
        folded into the per-job taxonomy."""
        job_id = UUID(str(new_job_id()))
        buffer = _dirty_buffer(job_id, state={"step": "one"})
        buffers: dict[UUID, _ProgressBuffer] = {job_id: buffer}

        with structlog.testing.capture_logs() as logs:
            await _flush_buffer(
                _FailingPool(),  # type: ignore[arg-type]  # Why: a minimal stand-in for asyncpg.Pool; only acquire() is reached.
                "taskq",
                job_id,
                new_uuid(),
                buffer,
                buffers,
            )

        dps = counter_data_points(otel_reader, "taskq.progress.flush_failures")
        assert dps and dps[0].attributes == {
            "stage": "pool",
            "error_type": "PostgresConnectionError",
        }, (
            "an acquire failure is a pool-wide outage (every job's flush "
            f"that tick is lost) and must carry stage='pool': {dps}"
        )
        kinds = {e.get("kind") for e in logs if e.get("kind")}
        assert kinds == {"progress_flush_pool_error"}, (
            f"the acquire failure must log the pool kind, not the per-job kind: {logs}"
        )

    async def test_pool_getter_failure_increments_counter(
        self, otel_reader: InMemoryMetricReader
    ) -> None:
        """A pool_getter that raises is a whole-loop outage and must also be counted."""
        job_id = UUID(str(new_job_id()))
        buffers: dict[UUID, _ProgressBuffer] = {job_id: _dirty_buffer(job_id)}
        shutdown = asyncio.Event()

        def _broken_pool_getter() -> Any:
            shutdown.set()
            raise RuntimeError("no pool available")

        await progress_flush_loop(
            _broken_pool_getter,
            "taskq",
            new_uuid(),
            buffers,
            0.001,
            shutdown,
        )

        assert counter_value(otel_reader, "taskq.progress.flush_failures") == 1

    async def test_pool_failure_is_distinguishable_from_per_job_failure(self) -> None:
        """The two handlers must not share one event name and kind.

        A per-job flush failure (the UPDATE statement fails) and a total
        inability to obtain a pool are materially different incidents:
        the first loses one job's progress, the second loses every job's.
        Emitting the identical ``"progress-flush-error"`` /
        ``kind="progress_flush_error"`` makes them indistinguishable in a
        log query or an alert rule.
        """
        job_id = UUID(str(new_job_id()))
        buffers: dict[UUID, _ProgressBuffer] = {job_id: _dirty_buffer(job_id)}
        shutdown = asyncio.Event()

        def _broken_pool_getter() -> Any:
            shutdown.set()
            raise RuntimeError("no pool available")

        with structlog.testing.capture_logs() as loop_logs:
            await progress_flush_loop(
                _broken_pool_getter,
                "taskq",
                new_uuid(),
                buffers,
                0.001,
                shutdown,
            )

        job_id2 = UUID(str(new_job_id()))
        buffer2 = _dirty_buffer(job_id2, state={"step": "one"})
        buffers2: dict[UUID, _ProgressBuffer] = {job_id2: buffer2}

        with structlog.testing.capture_logs() as job_logs:
            await _flush_buffer(
                _StatementFailingPool(),  # type: ignore[arg-type]  # Why: a minimal stand-in for asyncpg.Pool; only acquire() and the conn's fetchrow are reached.
                "taskq",
                job_id2,
                new_uuid(),
                buffer2,
                buffers2,
            )

        loop_kinds = {e.get("kind") for e in loop_logs if e.get("kind")}
        job_kinds = {e.get("kind") for e in job_logs if e.get("kind")}
        assert loop_kinds, f"pool-getter failure emitted no kinded log line: {loop_logs}"
        assert job_kinds, f"per-job flush failure emitted no kinded log line: {job_logs}"
        assert loop_kinds.isdisjoint(job_kinds), (
            "a total pool outage and a single-job flush failure share the same "
            f"log kind: {loop_kinds & job_kinds}"
        )

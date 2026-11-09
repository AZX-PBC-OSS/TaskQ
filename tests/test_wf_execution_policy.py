"""Execution-policy resolution and ordinary retry behavior on real PostgreSQL."""

from __future__ import annotations

import asyncio
import enum
import json
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Literal, cast

import asyncpg
import pytest
from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq.backend._protocol import JobId
from taskq.exceptions import ReservedTimerNamespaceError, RetryAfter, Snooze
from taskq.ratelimit.refs import KeyedReservationRef
from taskq.ratelimit.registry import RateLimitRegistry
from taskq.ratelimit.reservation import ConcurrencyReservation
from taskq.retry import RetryOverride, RetryPolicy
from taskq.workflows import (
    CLEAR_POLICY,
    Chain,
    ExecutionPolicy,
    FiniteReservationCohort,
    FlowRunner,
    Promise,
    RetrySpec,
    Step,
    StepContext,
    WorkflowApp,
    WorkflowPolicyError,
    WorkflowRetryOverride,
    build,
    chain_source,
    chain_start,
)
from taskq.workflows.api._validate import WorkflowValidationError


class _Outcome(enum.Enum):
    OK = "ok"


class _Record(BaseModel):
    cohort: str


class _ProviderError(RuntimeError):
    pass


class _PermanentProviderError(_ProviderError):
    pass


def _body(
    calls: list[int],
    failure: Callable[[], BaseException] | None = None,
) -> Callable[[StepContext, object], Awaitable[_Outcome]]:
    async def body(ctx: StepContext, item: object) -> _Outcome:
        calls.append(len(calls) + 1)
        if failure is not None:
            raise failure()
        return _Outcome.OK

    return body


def _chain(
    body: Callable[..., Awaitable[_Outcome]],
    *,
    policy: ExecutionPolicy | None = None,
    default_policy: ExecutionPolicy | None = None,
) -> Chain:
    suffix = new_uuid().hex
    return Chain(
        name=f"policy-chain-{suffix}",
        start="work",
        steps={"work": Step(body=body, outcomes=_Outcome, policy=policy)},
        default_policy=default_policy,
    )


async def _drive(
    chain: Chain,
    pool: asyncpg.Pool,
    schema: str,
    *,
    source_policy: ExecutionPolicy | None = None,
) -> tuple[JobId, JobId, JobId]:
    suffix = new_uuid().hex
    app = WorkflowApp()
    name = f"policy_flow_{suffix}"
    source_policy = source_policy

    async def source(ctx: StepContext) -> None:
        await ctx.emit_batch(
            [chain_start(chain, {"cohort": "a"}, map_index=0, trace_id="record-0")],
            cursor={"page": 0},
        )

    @app.workflow(name)
    def flow() -> Promise[object]:
        return build(chain_source(chain, source, key="source", policy=source_policy))

    runner = FlowRunner(app.get(name), pool, schema)
    claim = await runner.create_flow()
    assert await runner.drive(claim.flow_id, tick=0.001, max_ticks=1000) == "terminal"
    rows = await pool.fetch(
        f'SELECT id, step_key, status::text AS status, error_class, error_message FROM "{schema}".jobs '  # noqa: S608 — validated fixture schema
        "WHERE id = $1 OR (metadata->>'flow_id')::uuid = $1",
        claim.flow_id,
    )
    by_key = {str(row["step_key"]): JobId(row["id"]) for row in rows}
    if "work" not in by_key or "source" not in by_key:
        raise AssertionError(
            f"policy run did not materialize its rows: {[dict(row) for row in rows]!r}"
        )
    return claim.flow_id, by_key["work"], by_key["source"]


async def _job(pool: asyncpg.Pool, schema: str, job_id: JobId) -> asyncpg.Record:
    row = await pool.fetchrow(f'SELECT * FROM "{schema}".jobs WHERE id = $1', job_id)  # noqa: S608
    assert row is not None
    return row


def _metadata(row: asyncpg.Record) -> dict[str, object]:
    raw = row["metadata"]
    decoded = json.loads(raw) if isinstance(raw, str) else raw
    assert isinstance(decoded, dict)
    return cast("dict[str, object]", decoded)


def _rungs(row: asyncpg.Record) -> list[float]:
    metadata = _metadata(row)
    raw = metadata.get("wf_repend", {})
    assert isinstance(raw, dict)
    entries = cast("dict[str, object]", raw)
    values: list[float] = []
    for key in sorted(entries, key=int):
        entry = entries[key]
        assert isinstance(entry, dict)
        values.append(float(cast("dict[str, object]", entry)["backoff"]))
    return values


def test_one_resolver_handles_precedence_clear_and_start_fork_agreement() -> None:
    guard = ConcurrencyReservation(
        "policy-default-reservation", slots=1, lease=timedelta(seconds=30)
    )
    overridden = ConcurrencyReservation(
        "policy-step-reservation", slots=2, lease=timedelta(seconds=30)
    )
    chain = Chain(
        name="target-resolution",
        start="one",
        steps={
            "one": Step(body=_body([]), outcomes=_Outcome, route=None),
            "two": Step(body=_body([]), outcomes=_Outcome, route=None),
        },
        default_policy=ExecutionPolicy(
            actor="default-actor",
            queue="default-queue",
            max_concurrent=1,
            reservations=(guard,),
            priority=2,
            start_to_close=timedelta(seconds=4),
        ),
        # Compatibility placement is lower than default_policy placement.
    )
    # Constructing a second, equivalent compatibility declaration is lawful.
    equivalent = Chain(
        name="equivalent",
        start="one",
        steps=chain.steps,
        actor="default-actor",
        queue="default-queue",
        default_policy=chain.default_policy,
    )
    assert equivalent.resolved_policy("one").actor == "default-actor"

    chain.steps["one"] = Step(
        body=chain.steps["one"].body,
        outcomes=_Outcome,
        policy=ExecutionPolicy(
            priority=9,
            reservations=(overridden,),
            start_to_close=CLEAR_POLICY,
        ),
    )
    started = chain_start(chain, {"cohort": "a"}, map_index=0, trace_id="r0")
    assert (started.actor, started.priority, started.start_to_close) == (
        "default-actor",
        9,
        None,
    )
    assert started.reservations == (overridden.name,)
    assert started.retry is None
    assert chain.resolved_policy("one", run_priority=12).priority == 12
    # None still inherits; CLEAR_POLICY alone removes an inherited option.
    assert chain.resolved_policy("two").start_to_close == timedelta(seconds=4)

    with pytest.raises(WorkflowPolicyError, match="contradicts"):
        Chain(
            name="ambiguous-placement",
            start="one",
            steps=chain.steps,
            actor="other-actor",
            default_policy=ExecutionPolicy(actor="default-actor"),
        )
    with pytest.raises(WorkflowPolicyError, match="time_budget"):
        RetrySpec(policy=RetryPolicy(time_budget=timedelta(seconds=1)))
    with pytest.raises(WorkflowPolicyError, match="indefinite"):
        RetrySpec(policy=RetryPolicy(kind="indefinite"))
    with pytest.raises(WorkflowPolicyError, match="non_consuming_limit"):
        RetrySpec(classifier=lambda _exc, _attempt: None)


def test_exception_mapping_is_subtype_precise_and_none_is_an_explicit_default() -> None:
    called = 0

    def fallback(_exc: BaseException, _attempt: int) -> RetryOverride:
        nonlocal called
        called += 1
        return RetryOverride(delay=timedelta(seconds=7))

    spec = RetrySpec(
        exceptions={_ProviderError: None, _PermanentProviderError: "permanent"},
        classifier=fallback,
        non_consuming_limit=1,
    )
    permanent = spec.classify(_PermanentProviderError("terminal"), 1)
    assert permanent is not None and permanent.kind == "non_retryable"
    assert called == 0
    # An ordinary ProviderError mapping to None does not fall through to fallback.
    assert spec.classify(_ProviderError("use policy"), 1) is None
    assert called == 0
    other = spec.classify(RuntimeError("other"), 1)
    assert other is not None and other.delay == timedelta(seconds=7)
    assert called == 1

    with pytest.raises(WorkflowPolicyError, match="retry_kind"):
        from taskq.workflows._policy import canonical_retry_kind

        canonical_retry_kind("permanant")


@pytest.mark.integration
@pytest.mark.parametrize(
    ("shape", "expected"),
    [
        ("exponential", [0.01, 0.02, 0.025]),
        ("linear", [0.01, 0.02, 0.025]),
        ("fixed", [0.01, 0.01, 0.01]),
    ],
)
async def test_policy_curve_is_the_live_ladder_and_row_stamp(
    wf_pool: asyncpg.Pool,
    wf_schema: str,
    shape: str,
    expected: list[float],
) -> None:
    calls: list[int] = []
    spec = RetrySpec(
        policy=RetryPolicy(
            max_attempts=4,
            backoff=cast('Literal["exponential", "linear", "fixed"]', shape),
            base=timedelta(seconds=0.01),
            cap=timedelta(seconds=0.025),
            jitter=0,
        )
    )
    flow_id, job_id, _source_id = await _drive(
        _chain(_body(calls, lambda: RuntimeError("retry")), policy=ExecutionPolicy(retry=spec)),
        wf_pool,
        wf_schema,
    )
    row = await _job(wf_pool, wf_schema, job_id)
    assert len(calls) == 4
    assert (row["status"], row["attempt"], row["max_attempts"]) == ("failed", 4, 4)
    assert (
        row["retry_backoff"],
        row["retry_base_seconds"],
        row["retry_cap_seconds"],
        row["retry_jitter"],
    ) == (
        shape,
        0.01,
        0.025,
        0,
    )
    assert _rungs(row) == pytest.approx(expected)
    print(
        "POLICY_CURVE_ROWS",
        {
            "shape": shape,
            "rungs": _rungs(row),
            "body_calls": len(calls),
            "attempt": row["attempt"],
            "max_attempts": row["max_attempts"],
        },
    )
    failed = await wf_pool.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".wf_step_ledger '  # noqa: S608
        "WHERE flow_id = $1 AND step_key = 'work' AND status = 'failed'",
        flow_id,
    )
    assert failed == 4


@pytest.mark.integration
async def test_classifier_terminal_and_safe_fallback_are_real_execution(
    wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    calls: list[int] = []
    permanent = RetrySpec(
        policy=RetryPolicy(max_attempts=5, base=timedelta(seconds=0.001), jitter=0),
        exceptions={_PermanentProviderError: "permanent"},
    )
    flow_id, job_id, _ = await _drive(
        _chain(
            _body(calls, lambda: _PermanentProviderError("terminal")),
            policy=ExecutionPolicy(retry=permanent),
        ),
        wf_pool,
        wf_schema,
    )
    row = await _job(wf_pool, wf_schema, job_id)
    # The classifier's terminal override does not rewrite the immutable declaration.
    assert (len(calls), row["attempt"], row["retry_kind"]) == (1, 1, "transient")
    failed = await wf_pool.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".wf_step_ledger WHERE flow_id = $1'  # noqa: S608
        " AND step_key = 'work' AND status = 'failed'",
        flow_id,
    )
    assert failed == 1

    fallback_calls: list[int] = []
    classifier_calls = 0

    def broken(_exc: BaseException, _attempt: int) -> None:
        nonlocal classifier_calls
        classifier_calls += 1
        raise RuntimeError("broken classifier")

    fallback_flow, fallback_job, _ = await _drive(
        _chain(
            _body(fallback_calls, lambda: RuntimeError("body failure")),
            policy=ExecutionPolicy(
                retry=RetrySpec(
                    policy=RetryPolicy(max_attempts=2, base=timedelta(seconds=0.001), jitter=0),
                    classifier=broken,
                    non_consuming_limit=1,
                )
            ),
        ),
        wf_pool,
        wf_schema,
    )
    fallback_row = await _job(wf_pool, wf_schema, fallback_job)
    assert (len(fallback_calls), classifier_calls, fallback_row["attempt"]) == (2, 2, 2)
    fallback_failed = await wf_pool.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".wf_step_ledger WHERE flow_id = $1'  # noqa: S608
        " AND step_key = 'work' AND status = 'failed'",
        fallback_flow,
    )
    assert fallback_failed == 2


@pytest.mark.integration
async def test_classifier_never_sees_a_deterministic_authoring_fault(
    wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    calls: list[int] = []
    classifier_calls = 0

    def classifier(_exc: BaseException, _attempt: int) -> None:
        nonlocal classifier_calls
        classifier_calls += 1
        return None

    def failure() -> BaseException:
        return ReservedTimerNamespaceError("reserved")

    _flow_id, job_id, _ = await _drive(
        _chain(
            _body(calls, failure),
            policy=ExecutionPolicy(retry=RetrySpec(classifier=classifier, non_consuming_limit=1)),
        ),
        wf_pool,
        wf_schema,
    )
    row = await _job(wf_pool, wf_schema, job_id)
    assert (len(calls), classifier_calls, row["attempt"]) == (1, 0, 1)
    assert row["error_class"] == "ReservedTimerNamespaceError"


@pytest.mark.integration
async def test_non_consuming_counter_is_bounded_and_never_consumes_attempt_budget(
    wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    calls: list[int] = []
    claimed_metadata: list[dict[str, object]] = []

    async def failing_with_row_witness(ctx: StepContext, item: object) -> _Outcome:
        raw = await wf_pool.fetchval(
            f'SELECT metadata FROM "{wf_schema}".jobs WHERE id = $1',  # noqa: S608
            ctx.job_id,
        )
        decoded = json.loads(raw) if isinstance(raw, str) else raw
        assert isinstance(decoded, dict)
        claimed_metadata.append(cast("dict[str, object]", decoded))
        calls.append(len(calls) + 1)
        raise _ProviderError("wait")

    def advertised(_exc: BaseException, _attempt: int) -> WorkflowRetryOverride:
        return WorkflowRetryOverride(delay=timedelta(seconds=0.001), consume_budget=False)

    try:
        flow_id, job_id, _ = await _drive(
            _chain(
                failing_with_row_witness,
                policy=ExecutionPolicy(
                    retry=RetrySpec(
                        policy=RetryPolicy(max_attempts=2),
                        classifier=advertised,
                        non_consuming_limit=3,
                    )
                ),
            ),
            wf_pool,
            wf_schema,
        )
    except Exception as exc:
        raise AssertionError(
            f"advertised retry failed with claimed row metadata {claimed_metadata!r}"
        ) from exc
    row = await _job(wf_pool, wf_schema, job_id)
    metadata = _metadata(row)
    retry = metadata["wf_retry"]
    assert isinstance(retry, dict)
    assert cast("dict[str, object]", retry)["non_consuming_count"] == 3
    assert (len(calls), row["status"], row["attempt"], row["max_attempts"]) == (
        3,
        "failed",
        1,
        2,
    )
    assert (row["snooze_count"], metadata.get("wf_repend")) == (0, None)
    assert row["error_class"] == "RetryDeferralBudgetExceeded"
    failed = await wf_pool.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".wf_step_ledger '  # noqa: S608
        "WHERE flow_id = $1 AND step_key = 'work' AND status = 'failed'",
        flow_id,
    )
    assert failed == 1  # the one terminal CAS; advertised waits had no failed ledger
    print(
        "POLICY_NON_CONSUMING_ROWS",
        {
            "body_calls": len(calls),
            "non_consuming_count": cast("dict[str, object]", retry)["non_consuming_count"],
            "row_attempt": row["attempt"],
            "max_attempts": row["max_attempts"],
            "snooze_count": row["snooze_count"],
            "terminal_failed_ledger": failed,
        },
    )


@pytest.mark.integration
async def test_non_consuming_success_and_malformed_counter_fail_closed(
    wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    calls: list[int] = []

    def advertised(_exc: BaseException, _attempt: int) -> WorkflowRetryOverride:
        return WorkflowRetryOverride(delay=timedelta(0), consume_budget=False)

    # Use a body that can stop failing without turning the success into another call.
    async def succeeds_after_two(ctx: StepContext, item: object) -> _Outcome:
        calls.append(len(calls) + 1)
        if len(calls) <= 2:
            raise _ProviderError("wait")
        return _Outcome.OK

    _flow_id, job_id, _ = await _drive(
        _chain(
            succeeds_after_two,
            policy=ExecutionPolicy(
                retry=RetrySpec(
                    classifier=advertised,
                    non_consuming_limit=4,
                )
            ),
        ),
        wf_pool,
        wf_schema,
    )
    row = await _job(wf_pool, wf_schema, job_id)
    metadata = _metadata(row)
    retry = metadata["wf_retry"]
    assert isinstance(retry, dict)
    assert (len(calls), row["status"], row["snooze_count"]) == (3, "succeeded", 0)
    assert cast("dict[str, object]", retry)["non_consuming_count"] == 2

    malformed_calls: list[int] = []
    chain = _chain(
        _body(malformed_calls, lambda: _ProviderError("wait")),
        policy=ExecutionPolicy(retry=RetrySpec(classifier=advertised, non_consuming_limit=2)),
    )
    suffix = new_uuid().hex
    app = WorkflowApp()
    name = f"malformed_counter_{suffix}"

    async def malformed_source(ctx: StepContext) -> None:
        await ctx.emit_batch(
            [chain_start(chain, {"cohort": "a"}, map_index=0, trace_id="r0")],
            cursor={"page": 0},
        )

    @app.workflow(name)
    def malformed_flow() -> Promise[object]:
        return build(chain_source(chain, malformed_source, key="source"))

    runner = FlowRunner(app.get(name), wf_pool, wf_schema)
    flow = await runner.create_flow()
    await runner.tick(flow.flow_id)
    job = await wf_pool.fetchrow(
        f'SELECT id FROM "{wf_schema}".jobs '  # noqa: S608
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'work'",
        flow.flow_id,
    )
    assert job is not None
    await wf_pool.execute(
        f'UPDATE "{wf_schema}".jobs SET metadata = jsonb_set('  # noqa: S608
        "metadata, '{wf_retry,non_consuming_count}', '\"bad\"'::jsonb, true) "
        "WHERE id = $1",
        job["id"],
    )
    with pytest.raises(ValueError, match="signed-int32"):
        await runner.tick(flow.flow_id)
    after = await _job(wf_pool, wf_schema, JobId(job["id"]))
    assert (after["status"], after["attempt"], after["snooze_count"]) == ("running", 1, 0)
    retry_after = _metadata(after)["wf_retry"]
    assert isinstance(retry_after, dict)
    assert cast("dict[str, object]", retry_after)["non_consuming_count"] == "bad"


@pytest.mark.integration
async def test_kind_beats_consuming_retry_after_and_legacy_snooze_is_unchanged(
    wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    calls: list[int] = []

    async def advertised_failure(ctx: StepContext, item: object) -> _Outcome:
        calls.append(len(calls) + 1)
        raise RetryAfter(timedelta(milliseconds=5))

    _flow_id, job_id, _ = await _drive(
        _chain(
            advertised_failure,
            policy=ExecutionPolicy(
                retry=RetrySpec(policy=RetryPolicy(kind="non_retryable", max_attempts=3))
            ),
        ),
        wf_pool,
        wf_schema,
    )
    row = await _job(wf_pool, wf_schema, job_id)
    assert (len(calls), row["status"], row["attempt"]) == (1, "failed", 1)
    scheduled = await wf_pool.fetchval(
        f"SELECT count(*) FROM \"{wf_schema}\".jobs WHERE id = $1 AND status = 'scheduled'"  # noqa: S608
        " OR (id = $1 AND status = 'pending')",
        job_id,
    )
    assert scheduled == 0

    snooze_calls: list[int] = []

    async def ordinary_snooze(ctx: StepContext, item: object) -> _Outcome:
        snooze_calls.append(len(snooze_calls) + 1)
        if len(snooze_calls) < 3:
            raise Snooze(timedelta(0))
        return _Outcome.OK

    _legacy_flow, legacy_job, _ = await _drive(_chain(ordinary_snooze), wf_pool, wf_schema)
    legacy_row = await _job(wf_pool, wf_schema, legacy_job)
    assert (len(snooze_calls), legacy_row["status"], legacy_row["snooze_count"]) == (
        3,
        "succeeded",
        2,
    )
    assert "wf_retry" not in _metadata(legacy_row)


@pytest.mark.integration
async def test_row_policy_priority_deadline_and_source_metadata_execute(
    wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    entered = 0
    cancelled = 0

    async def slow(ctx: StepContext, item: object) -> _Outcome:
        nonlocal entered, cancelled
        entered += 1
        try:
            import asyncio

            await asyncio.sleep(1)
        finally:
            cancelled += 1
        return _Outcome.OK

    spec = RetrySpec(policy=RetryPolicy(max_attempts=1))
    guard = ConcurrencyReservation("row-policy-reservation", slots=1, lease=timedelta(seconds=30))
    chain = _chain(
        slow,
        policy=ExecutionPolicy(
            actor="row-policy-actor",
            queue="row-policy-queue",
            priority=7,
            reservations=(guard,),
            start_to_close=timedelta(milliseconds=10),
            retry=spec,
        ),
    )
    _flow_id, job_id, source_id = await _drive(
        chain,
        wf_pool,
        wf_schema,
        source_policy=ExecutionPolicy(
            actor="source-policy-actor",
            queue="source-policy-queue",
            priority=4,
            max_concurrent=1,
            retry=RetrySpec(policy=RetryPolicy(kind="non_retryable", max_attempts=5)),
        ),
    )
    row = await _job(wf_pool, wf_schema, job_id)
    source = await _job(wf_pool, wf_schema, source_id)
    assert (entered, cancelled, row["status"], row["attempt"]) == (1, 1, "failed", 1)
    assert (row["actor"], row["queue"], row["priority"]) == (
        "row-policy-actor",
        "row-policy-queue",
        7,
    )
    assert row["start_to_close"] == timedelta(milliseconds=10)
    assert row["error_class"] == "TimeoutError"
    assert _metadata(row)["reservations"] == [guard.name]
    assert (source["actor"], source["queue"], source["priority"], source["max_attempts"]) == (
        "source-policy-actor",
        "source-policy-queue",
        4,
        5,
    )
    assert source["retry_kind"] == "non_retryable"
    assert _metadata(source)["wf_source_logical_attempt"] == 1
    # The source did not silently inherit the record leaf's placement.
    assert source["actor"] != row["actor"]


def test_keyed_refs_resolve_stably_and_register_at_boot() -> None:
    ref = KeyedReservationRef.typed(
        _Record,
        base_name="record-cohort",
        key_fn=lambda record: record.cohort,
        slots=1,
        lease=timedelta(seconds=30),
    )
    finite = FiniteReservationCohort(ref=ref, keys=("finite-a", "finite-b"))
    chain = _chain(_body([]), policy=ExecutionPolicy(reservations=(finite,)))
    first = chain_start(chain, _Record(cohort="finite-a"), map_index=0, trace_id="r0")
    second = chain_start(chain, _Record(cohort="finite-a"), map_index=0, trace_id="r0")
    assert first.reservations == second.reservations == ("record-cohort:finite-a",)
    with pytest.raises(WorkflowPolicyError, match="outside the finite cohort"):
        chain_start(chain, _Record(cohort="not-declared"), map_index=1, trace_id="r1")
    app = WorkflowApp()
    name = f"keyed_policy_{new_uuid().hex}"

    async def source(ctx: StepContext) -> None:
        return None

    @app.workflow(name)
    def flow() -> Promise[object]:
        return build(chain_source(chain, source, key="source"))

    compiled = app.get(name)
    from taskq.workflows._worker_execution import (
        collect_workflow_reservations,
        record_compiled,
    )

    record_compiled(name, compiled)
    registry = RateLimitRegistry()
    reservations, unknown = collect_workflow_reservations(registry)
    assert unknown == []
    assert sorted(reservation.name for reservation in reservations) == [
        "record-cohort:finite-a",
        "record-cohort:finite-b",
    ]
    for reservation in reservations:
        registry.register(reservation)
    unresolved = asyncio.run(
        registry.materialize_keyed_reservations(first.reservations, settings=None)
    )
    assert unresolved == []
    assert registry.get_reservation(first.reservations[0]).slots == 1


@pytest.mark.parametrize("with_deps", [False, True])
def test_chain_arity_and_unbound_deps_are_named_build_refusals(with_deps: bool) -> None:
    app = WorkflowApp()

    wrong: Callable[..., Awaitable[_Outcome]]
    if with_deps:

        async def wrong_with_deps(ctx: StepContext, item: object, deps: object) -> _Outcome:
            return _Outcome.OK

        wrong = wrong_with_deps
    else:

        async def wrong_arity(ctx: StepContext) -> _Outcome:
            return _Outcome.OK

        wrong = wrong_arity

    chain = _chain(wrong)
    app = WorkflowApp()
    name = f"bad_chain_arity_{with_deps}_{new_uuid().hex}"

    async def source(ctx: StepContext) -> None:
        return None

    @app.workflow(name)
    def flow() -> Promise[object]:
        return build(chain_source(chain, source, key="source"))

    with pytest.raises(WorkflowValidationError) as caught:
        app.get(name)
    expected = "E12-deps-contract" if with_deps else "E10-arity"
    assert expected in str(caught.value)

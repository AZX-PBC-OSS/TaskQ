"""The type-only loop carry declaration is RUNNABLE: the pre-first-carry
``None`` passes through the carry contract.

``loop(..., carry_type=Model)``'s documented face — "an explicit
``carry_type=`` is for the type-only declaration (the initial carry is
``None``)" — was blessed but unrunnable: the driver seeds ``carry = None``
at iteration 0 (``_runner_loop``'s init) and re-hydrates UNCONDITIONALLY at
every iteration boundary, while ``rehydrate_carry`` refused ``None`` for a
``BaseModel`` declaration (``LoopCarryContractError`` — ``None`` is not a
dict). Every loop using the documented form failed on its FIRST iteration,
and no control drove that face.

The fix (in the contract's one owner, the validator): ``None`` is the
legitimate PRE-FIRST-CARRY value — it passes through as ``None`` because no
carry has been COMMITTED to the row yet; the driver supplies the provenance
(``committed=``) per face — the row read derives it from the iteration
counter the atomic advance owns (only the driver's own init/advance under
the claim fence writes that pair, so iteration 0 ⟺ nothing committed TO
THE ROW — a recorded iteration terminal the advance never reached, the
crash window's memo, is still a commit and the memo replay supplies
``committed=True`` itself), the memo replay and the in-memory ``Refine``
are commits by construction, and a body cannot mark its own feedback as a
seed. The no-silent-type-change
law is unchanged: a wrong-typed non-``None`` value still refuses loudly,
and a dict is still validated through the declared model. A COMMITTED
``None`` (a body's ``Refine(None)``) refuses loudly at the next boundary —
the seed is the only ``None`` the contract passes — while a JSON-native
declared type keeps the untyped ``None`` (the jsonb boundary honors the
declaration). Resumes stay correct: a resume before any carry was
committed also yields ``None`` (the init's seed is what the row reads
back); a resume after a ``Refine`` yields the typed instance — or the
refusal when the committed carry is ``None``.

Real driver (``FlowRunner.drive``), real PostgreSQL; the deferral-resume
arm re-claims the row through the real NODE_DEFER path, and the
crash-window control reconstructs the memo-replay face through the
product's own writers (the iteration-0 ledger claim plus the driver's own
iteration-terminal writer, no advance) — never hand-written memo JSON."""

from __future__ import annotations

from datetime import timedelta
from typing import Any, cast

import asyncpg
import pytest
from pydantic import BaseModel

from taskq.workflows import (
    Done,
    FlowRunner,
    Promise,
    Refine,
    StepContext,
    WorkflowApp,
    build,
    loop,
)
from taskq.workflows.api._runner_codec import LoopCarryContractError, rehydrate_carry

pytestmark = pytest.mark.integration


class Counter(BaseModel):
    """The declared carrier (the type-only declaration's subject)."""

    acc: int = 0


def _loop_app(
    body: Any,
    *,
    carry_type: type[BaseModel] | type[str] | type[int] | None = None,
    initial: object | None = None,
    max_iterations: int | None = None,
) -> tuple[WorkflowApp, str]:
    app = WorkflowApp()

    @app.workflow("counter_flow")
    def counter_flow() -> Promise[object]:
        return build(
            loop(
                "counter",
                body,
                # the JSON-native carrier face (the codec's declared
                # dict/list/str/int/float/bool branch — the opposite
                # control drives it): the loop door's annotation narrows
                # to models; the cast states the runtime-legal
                # declaration.
                carry_type=cast("type[BaseModel] | None", carry_type),
                initial=initial,
                max_iterations=max_iterations,
            )
        )

    app.get("counter_flow")  # compile + register
    return app, "counter_flow"


# ── (a) the type-only declaration runs: None at iteration 0, the model after the first Refine ──


async def test_type_only_carry_declaration_runs_and_observes_none_then_the_model(
    wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    """``carry_type=Counter`` with the seeded ``None`` initial: the body
    observes ``None`` at iteration 0 (the pre-first-carry value), the
    typed instance after the first ``Refine``, and the loop completes.
    On the pre-fix shape the FIRST iteration raised
    ``LoopCarryContractError`` (``None`` arrived where the declared
    model was demanded) and no iteration ever ran."""
    carries: list[object] = []

    async def body(ctx: StepContext, carry: object) -> object:
        carries.append(carry)
        if carry is None:
            # the first iteration: no carry exists yet — the Refine
            # commits the first one.
            return Refine(Counter(acc=1))
        assert isinstance(carry, Counter), (
            f"the carry arrived as {type(carry).__name__!r} — the declared "
            "type must reach the body once a carry exists"
        )
        acc = carry.acc + 1
        if acc >= 3:
            return Done(Counter(acc=acc))
        return Refine(Counter(acc=acc))

    app, name = _loop_app(body, carry_type=Counter, initial=None, max_iterations=10)
    runner = FlowRunner(app.get(name), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id) == "terminal"
    assert len(carries) == 3, carries
    assert carries[0] is None, (
        f"iteration 0's carry must be the pre-first-carry None, got {carries[0]!r}"
    )
    assert isinstance(carries[1], Counter) and carries[1].acc == 1, carries[1]
    assert isinstance(carries[2], Counter) and carries[2].acc == 2, carries[2]
    assert await runner.result(flow_id) == {"acc": 3}


# ── (b) a resume before the first carry was committed also observes None ────


async def test_resume_before_the_first_carry_committed_observes_none(
    wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    """The row-read resume face: the loop's FIRST claim defers through the
    real Snooze arm (the row law — the counters untouched, the advance
    never ran, the carry still the init's ``None``), the re-claim
    re-runs iteration 0, and the body observes ``None`` AGAIN before the
    Refine. On the pre-fix shape the resumed claim refused the same
    ``None`` the init had committed — the declaration could never be
    resumed either."""
    from taskq.exceptions import Snooze

    carries: list[object] = []
    calls: list[int] = []

    async def body(ctx: StepContext, carry: object) -> object:
        calls.append(1)
        carries.append(carry)
        if len(carries) == 1:
            # the first claim defers BEFORE any Refine ran: no carry was
            # ever committed, the row's carry stays the init's None.
            raise Snooze(timedelta(seconds=0))
        if carry is None:
            return Refine(Counter(acc=1))
        assert isinstance(carry, Counter)
        acc = carry.acc + 1
        if acc >= 3:
            return Done(Counter(acc=acc))
        return Refine(Counter(acc=acc))

    app, name = _loop_app(body, carry_type=Counter, initial=None, max_iterations=10)
    runner = FlowRunner(app.get(name), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id) == "terminal"
    assert len(carries) == 4, carries
    assert carries[0] is None and carries[1] is None, (
        f"both the first claim AND the resume before the first committed "
        f"carry must observe None, got {carries[:2]!r}"
    )
    assert isinstance(carries[2], Counter) and carries[2].acc == 1, carries[2]
    assert await runner.result(flow_id) == {"acc": 3}


# ── (c) the no-silent-type-change law is NOT weakened ────────────────────


async def test_wrong_typed_carry_still_refuses_loudly(
    wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    """A wrong-typed NON-None carry still refuses loudly — the pre-first-
    carry pass-through never laundered a real type change. The unit face
    (the validator's own contract): a non-dict raw refuses with
    ``LoopCarryContractError``; a dict is still validated THROUGH the
    declared model (the model's own validator refuses a wrong shape).
    The real-driver face: the init's list where the declared model was
    declared refuses at the first iteration boundary."""
    from pydantic import ValidationError

    with pytest.raises(LoopCarryContractError):
        rehydrate_carry(Counter, [1, 2], committed=True)
    with pytest.raises(LoopCarryContractError):
        rehydrate_carry(Counter, "a scalar the model cannot honor", committed=True)
    with pytest.raises(ValidationError):
        rehydrate_carry(Counter, {"acc": "not-an-int"}, committed=True)

    app, name = _loop_app(
        None,  # the body never runs — the rehydrate refuses first
        carry_type=Counter,
        initial=[1, 2],
        max_iterations=3,
    )
    runner = FlowRunner(app.get(name), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    with pytest.raises(LoopCarryContractError):
        await runner.drive(flow_id)


# ── (d) a dict carry without a declared model behaves exactly as before ─────


async def test_dict_carry_without_a_declared_model_is_unchanged(
    wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    """The untyped face: no ``carry_type`` — the raw dict passes at every
    boundary (the pre-typed-loop contract, unchanged by the None
    pass-through)."""
    carries: list[object] = []

    async def body(ctx: StepContext, carry: object) -> object:
        carries.append(carry)
        if len(carries) >= 3:
            return Done({"n": len(carries)})
        return Refine({"n": len(carries)})

    app, name = _loop_app(body, initial={"n": 0}, max_iterations=10)
    runner = FlowRunner(app.get(name), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id) == "terminal"
    assert len(carries) == 3, carries
    assert all(isinstance(c, dict) for c in carries), carries
    assert carries[0] == {"n": 0}, carries[0]
    assert carries[1] == {"n": 1} and carries[2] == {"n": 2}, carries[1:]


# ── (e) a COMMITTED None carry refuses loudly (the seed is the only None) ────


async def test_a_committed_none_carry_refuses_loudly(wf_pool: asyncpg.Pool, wf_schema: str) -> None:
    """A body's ``Refine(None)`` is a COMMITTED carry: the advance writes
    ``carry = null`` + ``iteration = 1``, and the NEXT boundary
    re-hydrates that committed value against the declared model — the
    loud ``LoopCarryContractError`` (the pre-first-carry seed is the
    only ``None`` the contract passes). On the pre-fix shape the
    ``None`` pass-through silently accepted it: the loop ran on,
    handing the typed body ``None`` again. The refusal names the
    arrival type, and the loop must not silently proceed — exactly one
    body invocation precedes it.

    The RESUME face drives the same law from the row read: the
    committed ``None`` is the real advance's own bytes (the crash
    window's row, re-claimable through the orphan-row pattern the
    suite's loop pins use); a fresh claim reads ``iteration = 1`` +
    ``carry = null`` and refuses BEFORE any body invocation."""
    carries: list[object] = []

    async def body(ctx: StepContext, carry: object) -> object:
        carries.append(carry)
        return Refine(None)

    app, name = _loop_app(body, carry_type=Counter, initial=None, max_iterations=10)
    runner = FlowRunner(app.get(name), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    with pytest.raises(LoopCarryContractError) as excinfo:
        await runner.drive(flow_id)
    assert "arrived as 'NoneType'" in str(excinfo.value), excinfo.value
    assert carries == [None], (
        f"exactly one body invocation may precede the refusal, got {carries!r}"
    )

    # the resume face: the row carries the committed ``None`` behind a
    # lapsed claim; the re-claim re-hydrates it from the ROW READ and
    # refuses again — before any body invocation.
    async with wf_pool.acquire() as conn:
        await conn.execute(
            f"UPDATE \"{wf_schema}\".jobs SET status = 'pending', "
            "locked_by_worker = NULL, lock_expires_at = NULL "
            "WHERE step_key = 'counter' AND (metadata->>'flow_id')::uuid = $1",
            flow_id,
        )
    with pytest.raises(LoopCarryContractError) as resume_exc:
        await runner.drive(flow_id)
    assert "arrived as 'NoneType'" in str(resume_exc.value), resume_exc.value
    assert carries == [None], (
        f"the resume face must refuse before any body invocation, got {carries!r}"
    )

    # the contract's own face, unit-pinned: the committed ``None`` is the
    # refusal (the row-read face's exact call at iteration >= 1 — the
    # dict branch shared by every dict subclass); the SEED
    # (committed=False) is the only ``None`` that passes.
    with pytest.raises(LoopCarryContractError):
        rehydrate_carry(Counter, None, committed=True)
    with pytest.raises(LoopCarryContractError):
        rehydrate_carry(dict, None, committed=True)
    assert rehydrate_carry(Counter, None, committed=False) is None


# ── (f) the opposite direction: a JSON-native declared type keeps the None ───


async def test_a_committed_none_passes_a_json_native_declaration(
    wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    """The over-tighten guard: a JSON-NATIVE declared type's committed
    ``None`` passes — exactly the pre-fix face (the jsonb boundary
    HONORS the declaration, so a ``None`` is no type change to refuse).
    The unit face pins every JSON-native carrier; the real-driver face
    runs the ``str`` loop: the committed ``None`` threads, the loop
    completes, no refusal."""
    for declared in (str, int, list, float, bool):
        assert rehydrate_carry(declared, None, committed=True) is None, declared
        assert rehydrate_carry(declared, None, committed=False) is None, declared

    carries: list[object] = []

    async def body(ctx: StepContext, carry: object) -> object:
        carries.append(carry)
        if len(carries) >= 3:
            return Done("counted")
        return Refine(None)

    app, name = _loop_app(body, carry_type=str, initial=None, max_iterations=10)
    runner = FlowRunner(app.get(name), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id) == "terminal"
    assert carries == [None, None, None], carries
    assert await runner.result(flow_id) == "counted"


# ── (g) the crash window: a memo-recorded Refine(None) refuses before the body ──


async def test_crash_window_memo_replay_of_a_committed_none_refuses_before_the_body(
    wf_pool: asyncpg.Pool, wf_schema: str, wf_conn: asyncpg.Connection
) -> None:
    """The MEMO face's provenance pin — the crash window between the
    iteration's terminal record and the advance. The iteration-0 ledger
    claim plus the product's own terminal writer have recorded the
    body's ``Refine(None)`` (the memo ``{"done": false, "feedback":
    null}``) and the driver died BEFORE the advance: no advance has
    landed. A fresh ``drive`` hits the memo replay at iteration 0 —
    where the row-read provenance is ``False`` and the recorded feedback
    is the ledger's own committed terminal — so the replay must supply
    ``committed=True`` itself and the boundary must refuse loudly
    against the declared model BEFORE any body invocation (0 calls: the
    replay advances from the memo, it never re-runs the recorded
    iteration). Deleting the memo face's ``carry_committed = True``
    re-seeds the recorded commit — the typed body receives the committed
    ``None`` again and the loop silently completes (the committed-``None``
    refusal's exact symptom, on this face) — while every other control
    in this module stays green: this pin exists for that line alone.

    The window is reconstructed through the product's own writers only —
    ``claim_step_ledger`` (the iteration-0 ledger claim; the synthetic
    caller's documented 0/0 stamps) and ``_record_iteration_terminal``
    (the terminal write the body's ``Refine(None)`` produces) — the memo
    is never hand-written JSON; the ledger facts asserted below, read
    back through the product's own memo reader (the seam the fresh
    drive itself hits), are what make the state the crash window rather
    than a fabrication."""
    from taskq._ids import JobId
    from taskq.workflows.ledger import claim_step_ledger, memoized_step_result

    calls: list[object] = []

    async def body(ctx: StepContext, carry_arg: object) -> object:
        calls.append(carry_arg)
        return Done(Counter(acc=9))

    app, name = _loop_app(body, carry_type=Counter, initial=None, max_iterations=10)
    runner = FlowRunner(app.get(name), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    row = await wf_conn.fetchrow(
        f"SELECT id, metadata->>'iteration' AS iteration FROM \"{wf_schema}\".jobs "
        "WHERE step_key = 'counter' AND (metadata->>'flow_id')::uuid = $1",
        flow_id,
    )
    assert row is not None
    # the crash window's first half, through the product's own writers:
    # the iteration-0 ledger claim, then the terminal record the body's
    # ``Refine(None)`` produces — and NO advance (the driver's death).
    async with wf_pool.acquire() as conn:
        claim = await claim_step_ledger(
            conn,
            runner.wsql,
            flow_id=flow_id,
            job_id=JobId(row["id"]),
            step_key="counter.iter0",
            map_index=None,
            attempt=0,
            claim_epoch=0,
        )
    assert claim.status == "running", claim.status
    await runner._record_iteration_terminal(  # pyright: ignore[reportPrivateUsage]  # Why: the driver's own terminal writer is the subject — the crash window's memo must be its product, never hand-written JSON.
        flow_id,
        "counter.iter0",
        0,
        Refine(None),
        claim,
    )
    # the crash window's ledger facts: the memo is the ledger's own
    # terminal record (exactly the ``Refine(None)`` shape), read back
    # through the product's own memo reader.
    async with wf_pool.acquire() as conn:
        memo = await memoized_step_result(
            conn, runner.wsql, flow_id=flow_id, step_key="counter.iter0", map_index=None
        )
    assert memo is not None and memo.status == "succeeded", memo
    assert memo.result == {"done": False, "feedback": None}, memo.result
    # the crash window's row fact: NO ADVANCE HAS LANDED. Both durable
    # shapes mean exactly that — this fixture's pre-init NULL seed and the
    # genuine crash window's post-init `iteration='0'` row (the fresh
    # drive's init writes the zero ordinal; a written zero is a seed, not
    # an advance) — so the assertion accepts either. NOT a weakening: the
    # refusal's detection power lives in the raises + `calls == []` +
    # post-state assertions below (all of the M-mutants red identically
    # under both shapes); the disjunction only removes a future spurious
    # red if the fresh drive's init ever moves before this read.
    assert row["iteration"] in (None, "0"), dict(row)

    # the fresh drive: the memo replay must treat the recorded feedback
    # as COMMITTED and refuse before any body invocation.
    with pytest.raises(LoopCarryContractError) as crash_exc:
        await runner.drive(flow_id)
    assert "NoneType" in str(crash_exc.value), crash_exc.value
    assert calls == [], (
        f"the crash-window replay must refuse before any body invocation, got {calls!r}"
    )
    # the replay-advance is the row's only writer between the seed and
    # the refusal: the memo face moved the row itself (the recorded
    # feedback advanced it), the body never ran.
    replayed = await wf_conn.fetchrow(
        f"SELECT metadata->>'iteration' AS iteration, metadata->'carry' AS carry "
        f'FROM "{wf_schema}".jobs WHERE id = $1',
        row["id"],
    )
    assert replayed is not None
    assert replayed["iteration"] == "1", dict(replayed)
    assert replayed["carry"] == "null", dict(replayed)

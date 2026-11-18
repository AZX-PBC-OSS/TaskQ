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
carry exists yet; only an ACTUAL carry is validated. The no-silent-type-
change law is unchanged: a wrong-typed non-``None`` value still refuses
loudly, and a dict is still validated through the declared model. Resumes
stay correct: a resume before any carry was committed also yields ``None``
(the init's seed is what the row reads back); a resume after a ``Refine``
yields the typed instance.

Real driver (``FlowRunner.drive``), real PostgreSQL; the deferral-resume
arm re-claims the row through the real NODE_DEFER path."""

from __future__ import annotations

from datetime import timedelta
from typing import Any

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
    carry_type: type[BaseModel] | None = None,
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
                carry_type=carry_type,
                initial=initial,
                max_iterations=max_iterations,
            )
        )

    app.get("counter_flow")  # compile + register
    return app, "counter_flow"


# ── (a) the type-only declaration runs: None at iteration 0, the model after the first Refine ──


async def test_type_only_carry_declaration_runs_and_observes_none_then_the_model(
    wf_pool: Any, wf_schema: str
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
    wf_pool: Any, wf_schema: str
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


async def test_wrong_typed_carry_still_refuses_loudly(wf_pool: Any, wf_schema: str) -> None:
    """A wrong-typed NON-None carry still refuses loudly — the pre-first-
    carry pass-through never laundered a real type change. The unit face
    (the validator's own contract): a non-dict raw refuses with
    ``LoopCarryContractError``; a dict is still validated THROUGH the
    declared model (the model's own validator refuses a wrong shape).
    The real-driver face: the init's list where the declared model was
    declared refuses at the first iteration boundary."""
    from pydantic import ValidationError

    with pytest.raises(LoopCarryContractError):
        rehydrate_carry(Counter, [1, 2])
    with pytest.raises(LoopCarryContractError):
        rehydrate_carry(Counter, "a scalar the model cannot honor")
    with pytest.raises(ValidationError):
        rehydrate_carry(Counter, {"acc": "not-an-int"})

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
    wf_pool: Any, wf_schema: str
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

"""Typed Step/Chain execution-policy probes for both pinned checkers."""

from __future__ import annotations

import enum
from datetime import timedelta

from pydantic import BaseModel

from taskq.retry import RetryPolicy
from taskq.workflows import (
    Chain,
    ExecutionPolicy,
    ExhaustionEvent,
    Promise,
    RetrySpec,
    Step,
    StepContext,
    TerminalEffect,
    WorkflowApp,
    build,
    chain_source,
)


class Record(BaseModel):
    record_id: str


class OtherRecord(BaseModel):
    other_id: int


class Outcome(enum.Enum):
    OK = "ok"


async def accepts_record(ctx: StepContext, item: Record) -> Outcome:
    return Outcome.OK


async def accepts_other(ctx: StepContext, item: OtherRecord) -> Outcome:
    return Outcome.OK


def expects_record_step(value: Step[Record, Outcome]) -> None:
    assert value is not None


async def source(ctx: StepContext) -> None:
    return None


def positive_inference() -> None:
    policy = ExecutionPolicy(
        priority=2,
        start_to_close=timedelta(seconds=2),
        retry=RetrySpec(policy=RetryPolicy(max_attempts=2)),
    )
    effect_policy = ExecutionPolicy(
        actor="close-record",
        retry=RetrySpec(policy=RetryPolicy(max_attempts=1)),
    )
    effect = TerminalEffect(body=closes_record, policy=effect_policy)
    policy = ExecutionPolicy(
        priority=2,
        start_to_close=timedelta(seconds=2),
        retry=RetrySpec(policy=RetryPolicy(max_attempts=2), on_exhaustion=effect),
    )
    step = Step(body=accepts_record, outcomes=Outcome, policy=policy)
    expects_record_step(step)
    chain = Chain(
        name="probe-policy-chain",
        start="work",
        steps={"work": step},
        default_policy=policy,
    )
    app = WorkflowApp()

    @app.workflow("probe-policy-chain")
    def flow() -> Promise[None]:
        return build(chain_source(chain, source, key="source"))

    assert flow is not None


async def closes_record(ctx: StepContext, event: ExhaustionEvent) -> None:
    assert event.node_key


def wrong_record_is_refused() -> None:
    expects_record_step(
        Step(body=accepts_other, outcomes=Outcome)
    )  # MUST_ERROR(reportArgumentType, invalid-argument-type): the generic record/body contract is preserved


def wrong_policy_and_absent_terminal_hook_are_refused() -> None:
    ExecutionPolicy(
        retry=RetryPolicy()
    )  # MUST_ERROR(reportArgumentType, invalid-argument-type): workflow retry declarations are RetrySpec, not an implicit shared RetryPolicy

    def unsupported_effect(event: object) -> None:
        assert event is not None

    ExecutionPolicy(
        terminal_effect=unsupported_effect
    )  # MUST_ERROR(reportCallIssue, unknown-argument): the supported face is RetrySpec.on_exhaustion; this guessed alias remains refused

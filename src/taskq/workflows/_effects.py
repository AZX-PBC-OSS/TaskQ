"""Durable authored terminal-effect intents and their ordinary job bindings.

Exactly one intent and one queue row are physical-key/idempotency guarantees.
The Python body, external I/O, and provider effects are at-least-once. A domain
write completes once only when its dedupe marker and effects share one DB tx.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from taskq._ids import new_uuid
from taskq._sql_render import render
from taskq.backend._protocol import ConnLike, ErrorInfo, JobId
from taskq.workflows._policy import (
    ExhaustionEvent,
    ResolvedExecutionPolicy,
    TerminalEffect,
    TerminalEffectReason,
    resolve_execution_policy,
)
from taskq.workflows._types import (  # pyright: ignore[reportPrivateUsage]
    ConsumerBinding,
    _consumer_binding_doc,  # pyright: ignore[reportPrivateUsage]  # the binding's canonical internal wire writer
    _jsonb,
)

if TYPE_CHECKING:
    from taskq.workflows._sql import WorkflowSql
    from taskq.workflows.api._app import CompiledWorkflow

TERMINAL_EFFECT_SUFFIX = ".__on_exhaustion"
TERMINAL_EFFECT_METADATA = "wf_after_terminal_effect"
TERMINAL_EFFECT_KEY = "wf_effect_key"


@dataclass(frozen=True, slots=True)
class RegisteredTerminalEffect:
    source_key: str
    effect: TerminalEffect

    @property
    def step_key(self) -> str:
        return f"{self.source_key}{TERMINAL_EFFECT_SUFFIX}"


def collect_terminal_effects(
    compiled: CompiledWorkflow,
) -> dict[str, RegisteredTerminalEffect]:
    """The validated handler cohort shared by registration, boot and dispatch."""
    found: dict[str, RegisteredTerminalEffect] = {}
    candidates: list[tuple[str, TerminalEffect | None]] = []
    for key, node in compiled.nodes.items():
        resolved = getattr(node, "execution_policy", None)
        retry = resolved.retry if resolved is not None else None
        candidates.append((key, retry.on_exhaustion if retry is not None else None))
    for chain in compiled.chains:
        for key in chain.steps:
            retry = chain.resolved_policy(key, resolve_names=False).retry
            candidates.append((key, retry.on_exhaustion if retry is not None else None))
    for source_key, effect in candidates:
        if effect is None:
            continue
        registered = RegisteredTerminalEffect(source_key=source_key, effect=effect)
        if registered.step_key in found or registered.step_key in compiled.nodes:
            raise ValueError(
                f"terminal-effect handler key {registered.step_key!r} collides with another body"
            )
        found[registered.step_key] = registered
    return found


def effect_for_source(
    effects: dict[str, RegisteredTerminalEffect], source_key: str
) -> TerminalEffect | None:
    registered = effects.get(f"{source_key}{TERMINAL_EFFECT_SUFFIX}")
    return registered.effect if registered is not None else None


def terminal_effect_key(*, flow_id: JobId, run_epoch: int, node_id: JobId) -> str:
    """Stable identity for this run incarnation, node/record and one declaration."""
    if type(run_epoch) is not int or run_epoch < 1:
        raise ValueError("a guarded terminal effect requires a non-null positive run_epoch")
    # One on_exhaustion declaration per node makes the remaining effect identity
    # explicit without relying on Python module/qualname text as an SQL key.
    return f"{flow_id}:{run_epoch}:{node_id}:on_exhaustion"


def terminal_effect_consumer(
    *,
    flow_id: JobId,
    flow_name: str,
    node_id: JobId,
    source_key: str,
    map_index: int | None,
    attempt: int,
    run_epoch: int,
    trace_id: str | None,
    error: ErrorInfo,
    record: object,
    reason: TerminalEffectReason,
    effect: TerminalEffect,
) -> tuple[str, ConsumerBinding, str]:
    effect_key = terminal_effect_key(flow_id=flow_id, run_epoch=run_epoch, node_id=node_id)
    event = ExhaustionEvent(
        flow_id=flow_id,
        flow_name=flow_name,
        node_id=node_id,
        node_key=source_key,
        map_index=map_index,
        attempt=attempt,
        run_epoch=run_epoch,
        trace_id=trace_id,
        error=error,
        record=record,
        reason=reason,
    )
    actor = effect.policy.actor
    queue = effect.policy.queue
    assert isinstance(actor, str) and isinstance(queue, str)  # declaration validated
    resolved: ResolvedExecutionPolicy = resolve_execution_policy(
        step_policy=effect.policy,
        compatibility_actor=actor,
        compatibility_queue=queue,
        payload_values=(record,),
    )
    binding = ConsumerBinding(
        step_key=f"{source_key}{TERMINAL_EFFECT_SUFFIX}",
        actor=resolved.actor,
        queue=resolved.queue,
        payload=event.model_dump(mode="json"),
        map_index=map_index,
        start_to_close=resolved.start_to_close,
        rate_limits=resolved.rate_limits,
        reservations=resolved.reservations,
        priority=resolved.priority,
        retry=resolved.retry,
        effect_key=effect_key,
    )
    return binding.step_key, binding, effect_key


TERMINAL_EFFECT_INTENT_SQL = """\
INSERT INTO {schema}.wf_outbox
    (id, join_job_id, flow_id, consumer_step_key, map_index, bindings)
SELECT $1, $2, $3, $4, $5::bigint, $6::jsonb
ON CONFLICT ((bindings->>'wf_effect_key'))
    WHERE bindings ? 'wf_effect_key'
DO NOTHING
RETURNING id, (bindings->>'wf_effect_key') AS effect_key
"""


async def insert_terminal_effect(
    conn: ConnLike,
    wsql: WorkflowSql,
    *,
    flow_id: JobId,
    flow_name: str,
    node_id: JobId,
    source_key: str,
    map_index: int | None,
    attempt: int,
    run_epoch: int,
    trace_id: str | None,
    error_class: str | None,
    error_message: str | None,
    error_traceback: str | None,
    record: object,
    reason: TerminalEffectReason,
    effect: TerminalEffect,
) -> bool:
    """Insert the stable intent inside the caller's terminal/outbox lock domain."""
    step_key, binding, _effect_key = terminal_effect_consumer(
        flow_id=flow_id,
        flow_name=flow_name,
        node_id=node_id,
        source_key=source_key,
        map_index=map_index,
        attempt=attempt,
        run_epoch=run_epoch,
        trace_id=trace_id,
        error=ErrorInfo(
            error_class=error_class or "WorkflowExhausted",
            error_message=error_message or "workflow retry policy exhausted",
            error_traceback=error_traceback,
        ),
        record=record,
        reason=reason,
        effect=effect,
    )
    # The typed binding owns the wire writer; no arbitrary metadata or text SQL
    # can author the effect marker or its physical intent key.
    written = await conn.fetchval(
        render(TERMINAL_EFFECT_INTENT_SQL, wsql.schema),
        new_uuid(),
        node_id,
        flow_id,
        step_key,
        map_index,
        _jsonb(_consumer_binding_doc(binding)),
    )
    return written is not None


UNFINISHED_TERMINAL_EFFECTS_SQL = """\
SELECT EXISTS (
    SELECT 1 FROM {schema}.wf_outbox AS o
    WHERE o.flow_id = $1 AND NOT o.delivered AND o.bindings ? 'wf_effect_key'
) OR EXISTS (
    SELECT 1 FROM {schema}.jobs AS j
    WHERE (j.metadata->>'flow_id')::uuid = $1
      AND j.metadata ? 'wf_after_terminal_effect'
      AND j.status NOT IN ('succeeded','failed','cancelled','crashed','abandoned')
)
"""


async def has_unfinished_terminal_effects(
    conn: ConnLike, wsql: WorkflowSql, flow_id: JobId
) -> bool:
    """The shared P8-ready leg: pending intent AND active/unfinished execution."""
    return bool(await conn.fetchval(render(UNFINISHED_TERMINAL_EFFECTS_SQL, wsql.schema), flow_id))

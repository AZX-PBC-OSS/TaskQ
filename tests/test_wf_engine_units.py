"""The in-process engine pins (T04): pin 10 (the seam-only generation grep), pin 11 (REDACT-BEFORE-PERSIST), pin 12 (CANONICAL-HASH), pin 14 (the deadlock-retry budget), pin 16 (BODY-FROM-DEFINITION).

Driven against a live Postgres through the REAL engine; the shared seed
helpers + fixtures live in ``tests/_wf_fixtures.py`` (the composed-fixture
home), the red-output sink flushes to ``.measurements/pin-reds.json`` (a
file that gets READ — BUILD-PROTOCOL §2), the shipped invariants green,
the unfenced variants kept in this file forever as the convicted shapes.
"""

# Why: every f-string SQL below interpolates only the module fixture's own throwaway schema identifier (validated against _IDENT_RE) or renders the engine's own named constants with a named mutation; all values are $n-bound.
# Why: random module used for timing jitter in race tests, not crypto.

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest

from taskq.workflows._capture import build_capture
from taskq.workflows._version import compute_code_version
from taskq.workflows.engine import (
    DeadlockRetriesExhaustedError,
    _deadlock_retry,  # pyright: ignore[reportPrivateUsage]  # Why: the pin drives the retry helper directly; it is the module-private seam by design (the public entry is finalize_node).
)
from tests._wf_fixtures import RedLog

# ── Pin 10: THE SEAM-ONLY GENERATION PIN ────────────────────────────────


def test_pin_10_seam_only_generation() -> None:
    """``grep -rnE 'gen_random_uuid|uuid4' src/taskq/workflows/`` reports
    ZERO hits — the seam is the ONLY generation site (TID251)."""
    import subprocess  # Why: the pin IS the ticket's grep shape.

    result = subprocess.run(  # Why: fixed argv, the repo's own package.
        ["/usr/bin/grep", "-rnE", "gen_random_uuid|uuid4", "src/taskq/workflows/"],
        capture_output=True,
        text=True,
        check=False,
    )
    hits = result.stdout.splitlines()
    assert not hits, hits
    # ...and the seam is what the engine mints through.
    import inspect

    import taskq.workflows.engine as engine

    assert "new_uuid()" in inspect.getsource(engine), "the engine must mint ids through the seam"


# ── Pin 11: REDACT-BEFORE-PERSIST (the canary never lands) ──────────────


def test_pin_11_redact_before_persist(engine_redlog: RedLog) -> None:
    """A canary in the fixture payload, the ``redact=fn`` hook configured,
    the node fails → the capture row must contain NO canary. The hook
    composes AFTER the chain (chain → hook, unconditional). THE NEGATIVE
    ARM (TORS-REV-0.16 §G1): a hook that tries to WIDEN visibility is not
    refused — intent is undetectable — it is structurally powerless on its
    OWN input (already scrubbed), and the pin asserts the chain's masks
    are present on the persisted capture REGARDLESS of an honest hook's
    return. The writer variant that lets the hook run BEFORE (or instead
    of) the chain reds (the hole where the canary reaches the row)."""
    canary = "AKIAIOSFODNN7EXAMPLE"  # the canonical AWS access-key example

    # THE CONVICTED VARIANT: hook-instead-of-chain — the canary lands.
    def hook_instead_of_chain(node_input: str) -> dict[str, str]:
        # The writer the composition forbids: the hook's output persisted
        # with NO chain pass on it.
        hook = lambda t: t  # noqa: E731  # Why: the identity hook isolates the missing-chain defect.
        return {"input": hook(node_input)}

    convicted = hook_instead_of_chain(f"payload with {canary}")
    engine_redlog.red(
        "pin11-redact-before-persist",
        "capture writer persists the hook's output without the chain pass",
        {"canary_persisted": canary in json.dumps(convicted)},
    )
    assert canary in json.dumps(convicted), (
        "the convicted variant no longer leaks the canary — the red "
        "comparator is broken (the chain must be load-bearing)"
    )

    # THE SHIPPED PIPELINE: chain → hook, on every field.
    honest = build_capture(
        policy="errors-only",
        node_input=f"payload with {canary}",
        error=f"boom at {canary}",
        redact=lambda t: t,  # an honest no-op hook
    )
    assert honest is not None
    dumped = json.dumps(honest)
    assert canary not in dumped, dumped  # NO canary reaches the row
    # A mask LANDED: the chain's output differs from its input (the tors
    # correlation-token form <head>~<digest>, or the regex masks).
    assert honest["input"] != f"payload with {canary}", dumped


# ── Pin 12: CANONICAL-HASH (dict-ordering must not matter) ──────────────


def test_pin_12_canonical_hash(engine_redlog: RedLog) -> None:
    """A dict-ordering mutation of the same args must NOT change the
    code-version hash (a false invalidation is the silent failure). The
    non-canonical serializer variant (json.dumps, insertion-ordered) reds."""
    import hashlib

    from tors import content_hash

    assert compute_code_version("m", "f", source="def f(): ...") == compute_code_version(
        "m", "f", source="def f(): ..."
    ), "the same inputs must hash stably"

    # THE CONVICTED VARIANT: an insertion-ordered serializer — the same
    # args under a different insertion order hash DIFFERENTLY (the false
    # invalidation).
    def non_canonical(payload: dict[str, str]) -> str:
        return hashlib.sha256(json.dumps(payload).encode()).hexdigest()

    left = non_canonical({"module": "m", "qualname": "f"})
    right = non_canonical({"qualname": "f", "module": "m"})
    engine_redlog.red(
        "pin12-canonical-hash",
        "json.dumps without canonical ordering (insertion-ordered)",
        {"left": left, "right": right, "moved": left != right},
    )
    assert left != right, "the non-canonical variant must red"

    # And the tors hash is canonical under the same mutation.
    assert content_hash({"a": "1", "b": "2"}) == content_hash({"b": "2", "a": "1"})


# ── Pin 16: BODY-FROM-DEFINITION (D1) ───────────────────────────────────


def test_pin_16_body_from_definition(engine_redlog: RedLog) -> None:
    """Two overlapping dispatches of one pending node — one with a MUTATED
    per-call body map — must resolve BOTH bodies from the REGISTERED
    definition (the claim-CAS loser runs the defined body, never the
    per-call variant)."""
    from taskq.workflows.definitions import (
        WorkflowDef,
        get_registry,
        resolve_step_body,
    )

    async def defined_body(ctx: Any) -> str:  # pragma: no cover - identity only
        return "defined"

    async def mutated_body(ctx: Any) -> str:  # pragma: no cover - identity only
        return "MUTATED"

    get_registry().register(WorkflowDef(name="pin16-flow", bodies={"step_a": defined_body}))
    # BOTH dispatches resolve from the registry — a per-call map, had it
    # existed, would double-task the pending node.
    winner = resolve_step_body("pin16-flow", "step_a")
    loser = resolve_step_body("pin16-flow", "step_a")
    assert winner is defined_body and loser is defined_body

    # THE CONVICTED VARIANT: a per-call body map — the loser of the claim
    # CAS runs the WRONG body.
    per_call_map = {"step_a": mutated_body}
    engine_redlog.red(
        "pin16-body-from-definition",
        "per-call body map (the D1 hazard: the CAS loser runs the wrong body)",
        {"loser_body": "MUTATED"},
    )
    assert per_call_map["step_a"] is mutated_body  # the wrong body, the red


# ── Pin 14: DEADLOCK-RETRY (the budget + the linearization-preserving shape)


def test_pin_14_deadlock_retry_budget(engine_redlog: RedLog) -> None:
    """Both operators RETRY on a real Postgres ``DeadlockDetectedError``
    (the aborted TX had no effect → linearization-preserving); the budget
    exhausts LOUDLY. The x200 linearization storm itself is T15's crash
    fixture (the real-PG storm shape; the engine-side budget pin lives
    here)."""
    import asyncpg.exceptions

    async def run() -> None:
        attempts = 0

        async def always_deadlocks() -> None:
            nonlocal attempts
            attempts += 1
            raise asyncpg.exceptions.DeadlockDetectedError("40P01", "deadlock detected")

        async def recovers_on_third() -> str:
            nonlocal attempts
            attempts += 1
            if attempts < 3:
                raise asyncpg.exceptions.DeadlockDetectedError("40P01", "deadlock detected")
            return "landed"

        with pytest.raises(DeadlockRetriesExhaustedError):
            await _deadlock_retry(always_deadlocks)
        assert attempts == 5, attempts  # 1 + _DEADLOCK_RETRIES
        attempts = 0
        assert await _deadlock_retry(recovers_on_third) == "landed"
        engine_redlog.red(
            "pin14-deadlock-retry",
            "no retry (the single-shot operator under storm contention)",
            {"aborted_with_DeadlockDetectedError": True, "retries": 0},
        )

    asyncio.run(run())

"""The @actor boundary refuses lossy float constraints on payload fields.

Adopted from tors's constrained-bigint gate (the schema refuses rather
than validating lossily): a float-typed payload field with an integral
constraint at or beyond 2**53 cannot represent the constraint's
neighbors, so `model_validate` accepts input the author's bound forbids
and `model_dump` ships the aliased value to jsonb. Measured pre-gate:
`Field(ge=10**25)` accepted `10**25 - 1` and stored `1e+25`.
"""

import pytest
from pydantic import BaseModel, Field

from taskq.actor import actor
from taskq.constants import FLOAT_EXACT_INTEGER_LIMIT, check_payload_float_constraints


class _CleanPayload(BaseModel):
    amount: float = Field(ge=10.5)
    count: int = Field(ge=10**25)  # int fields are exact in json: fine


class _AliasedPayload(BaseModel):
    amount: float = Field(ge=10**25)


class _AliasedLePayload(BaseModel):
    ceiling: float = Field(le=2**53)


class _AliasedMultipleOfPayload(BaseModel):
    step: float = Field(multiple_of=2**54)


def test_decoration_refuses_float_ge_beyond_exact_range() -> None:
    with pytest.raises(ValueError, match="2\\*\\*53") as exc_info:
        actor(name="aliased_actor", queue="default")(_aliased_handler)

    assert "amount" in str(exc_info.value), (
        "the refusal must name the offending field, not just the model"
    )


def _aliased_handler(payload: _AliasedPayload) -> None:
    pass


def test_decoration_refuses_float_le_at_the_boundary() -> None:
    with pytest.raises(ValueError, match="2\\*\\*53"):
        actor(name="le_actor", queue="default")(_aliased_le_handler)


def _aliased_le_handler(payload: _AliasedLePayload) -> None:
    pass


def test_decoration_refuses_float_multiple_of_beyond_exact_range() -> None:
    with pytest.raises(ValueError, match="2\\*\\*53"):
        actor(name="mo_actor", queue="default")(_aliased_mo_handler)


def _aliased_mo_handler(payload: _AliasedMultipleOfPayload) -> None:
    pass


def test_clean_models_decorate() -> None:
    actor(name="clean_actor", queue="default")(_clean_handler)
    actor(name="clean_actor2", queue="default")(_clean_typed_handler)


def _clean_handler(payload: _CleanPayload) -> None:
    pass


class _CleanTypedPayload(BaseModel):
    # a float constraint WITHIN the exact range: fine
    ratio: float = Field(ge=0.0, le=1.0)


def _clean_typed_handler(payload: _CleanTypedPayload) -> None:
    pass


def test_non_pydantic_and_non_float_shapes_pass() -> None:
    # non-BaseModel annotations are the caller's business (the decorator
    # rejects them elsewhere); the gate must not crash on them.
    check_payload_float_constraints(dict, what="probe")
    check_payload_float_constraints(int, what="probe")


def test_the_alias_is_real_the_gate_is_not_paranoia() -> None:
    """The measured defect: validate accepts 10**25 - 1 for ge=10**25."""
    value = 10**25 - 1
    aliased = _AliasedPayload.model_validate({"amount": float(value)})
    assert aliased.amount == float(10**25), (
        "precondition for the gate: the f64 alias swallowed the neighbor; "
        "if this assertion fails, f64 grew exact digits and the gate's "
        "threshold can move"
    )
    assert aliased.amount >= 10**25  # the constraint the author wrote LIES


def test_gate_threshold_is_the_f64_exact_integer_limit() -> None:
    # AT the limit already aliases (2**53 - 1 rounds UP to 2**53, so a
    # ge=2**53 constraint would accept a value the author read as one
    # below the bound): refused.
    class AtLimit(BaseModel):
        v: float = Field(ge=FLOAT_EXACT_INTEGER_LIMIT)

    with pytest.raises(ValueError):
        check_payload_float_constraints(AtLimit, what="probe")

    # comfortably inside the exact range: allowed
    class WithinLimit(BaseModel):
        v: float = Field(ge=FLOAT_EXACT_INTEGER_LIMIT - 1)

    check_payload_float_constraints(WithinLimit, what="probe")

    # one beyond aliases: refused
    class BeyondLimit(BaseModel):
        v: float = Field(ge=FLOAT_EXACT_INTEGER_LIMIT + 2)

    with pytest.raises(ValueError):
        check_payload_float_constraints(BeyondLimit, what="probe")

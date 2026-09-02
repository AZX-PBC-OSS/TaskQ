"""Pins the public API facts that docs/guides/upgrading.md instructs on.

The upgrading guide tells consumers to rewrite ``actor_name=`` as
``actor=`` at ``taskq.validate_actor_payload`` call sites, and warns that
the exception no longer carries payload values or the pydantic ``input``
/ ``url`` keys. Those are load-bearing migration instructions: if the
signature or the error shape drifts back, the guide sends every consumer
of this release to the wrong call-site edit.

These assert the *API surface and behaviour* via ``inspect.signature``
and a real validation failure — they do not read the prose. The
sanitization itself (no payload values in the message) and the
single-implementation identity are already covered by
test_exceptions.py; what is pinned here is the parameter naming and
kind, and the absence of the ``input`` / ``url`` keys, which nothing else
asserts.

Contract-test precedent: test_max_concurrent_docs_contract.py.
"""

import inspect

import pytest
from pydantic import BaseModel

import taskq
import taskq.backend
from taskq.backend.statemachine import (
    ACTIVE_STATUSES,
    TERMINAL_STATUSES,
    VALID_TRANSITIONS,
    assert_valid_transition,
)
from taskq.exceptions import PayloadValidationError


class _IntFieldPayload(BaseModel):
    """Payload whose only field is an int — a str value fails validation."""

    count: int


def test_validate_actor_payload_third_parameter_is_named_actor() -> None:
    """The third parameter is ``actor`` — the rename upgrading.md documents.

    ``actor_name`` was the pre-rename name in the deleted exceptions.py
    duplicate; it must not come back, or the guide's before/after is
    inverted.
    """
    params = list(inspect.signature(taskq.validate_actor_payload).parameters)

    assert params[2] == "actor"
    assert "actor_name" not in params


def test_validate_actor_payload_accepts_actor_keyword() -> None:
    """The ``actor=`` keyword form the guide tells consumers to migrate TO."""
    with pytest.raises(PayloadValidationError) as exc_info:
        taskq.validate_actor_payload(_IntFieldPayload, {"count": "x"}, actor="send_email")

    assert exc_info.value.actor == "send_email"


def test_validate_actor_payload_accepts_third_positional() -> None:
    """The positional form the guide promises is unaffected by the rename."""
    with pytest.raises(PayloadValidationError) as exc_info:
        taskq.validate_actor_payload(_IntFieldPayload, {"count": "x"}, "send_email")

    assert exc_info.value.actor == "send_email"


def test_validate_actor_payload_actor_is_optional() -> None:
    """``actor`` defaults to ``None`` — omitting it no longer raises TypeError."""
    assert inspect.signature(taskq.validate_actor_payload).parameters["actor"].default is None

    with pytest.raises(PayloadValidationError) as exc_info:
        taskq.validate_actor_payload(_IntFieldPayload, {"count": "x"})

    assert exc_info.value.actor is None


def test_validation_errors_omit_input_and_url_keys() -> None:
    """``validation_errors`` entries carry neither ``input`` nor ``url``.

    ``include_input=False`` is what keeps attacker-controlled values out of
    the persisted ``error_message`` / web admin; ``include_url=False`` drops
    the pydantic docs link. The guide warns that ``err["input"]`` now raises
    ``KeyError``, so pin that it is genuinely absent.
    """
    with pytest.raises(PayloadValidationError) as exc_info:
        taskq.validate_actor_payload(_IntFieldPayload, {"count": "x"}, actor="probe")

    errors = exc_info.value.validation_errors
    assert errors, "expected at least one field error"
    for err in errors:
        assert "input" not in err
        assert "url" not in err


def test_validate_actor_payload_accepts_a_basemodel_as_raw_payload() -> None:
    """The widened ``raw_payload`` the guide notes needs no consumer action."""
    validated = taskq.validate_actor_payload(_IntFieldPayload, _IntFieldPayload(count=3))

    assert isinstance(validated, _IntFieldPayload)
    assert validated.count == 3


# ── State-machine constants via taskq.backend (upgrading.md import note) ──


def test_terminal_statuses_reexport_is_the_statemachine_object() -> None:
    """``taskq.backend.TERMINAL_STATUSES`` is the exact object the internal
    ``taskq.backend.statemachine`` module defines — the identity the
    guide's before/after promises (a switch, not a copy)."""
    assert taskq.backend.TERMINAL_STATUSES is TERMINAL_STATUSES


def test_terminal_statuses_is_a_declared_backend_export() -> None:
    """The public path is a declared ``__all__`` entry, not an accidental
    attribute — what makes it the covered surface the guide points at."""
    assert "TERMINAL_STATUSES" in taskq.backend.__all__


def test_state_machine_constants_reexported_from_backend() -> None:
    """The other state-machine names the guide's note mentions are
    re-exported from ``taskq.backend`` as the same objects too."""
    pairs: list[tuple[str, object]] = [
        ("ACTIVE_STATUSES", ACTIVE_STATUSES),
        ("VALID_TRANSITIONS", VALID_TRANSITIONS),
        ("assert_valid_transition", assert_valid_transition),
    ]
    for name, internal_obj in pairs:
        assert getattr(taskq.backend, name) is internal_obj
        assert name in taskq.backend.__all__

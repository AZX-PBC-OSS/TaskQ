"""Shared payload validation helper."""

from typing import Final

from pydantic import BaseModel, ValidationError

from taskq.exceptions import PayloadValidationError

#: The payload schema version every row written today carries. Mirrors the
#: ``EnqueueArgs.payload_schema_ver`` field default (``backend/_protocol.py``)
#: as a literal rather than an import — the dataclass field default is not a
#: module-level constant, and this module stays importable by the driver-free
#: testing boundary (the same reason ``testing/_dispatch.py`` mirrors
#: dispatch constants as literals). Raise sites that hold a job row must
#: pass the ROW's stored version instead — see ``validate_actor_payload``'s
#: ``payload_schema_ver`` parameter.
CURRENT_PAYLOAD_SCHEMA_VER: Final[int] = 1


def validate_actor_payload(
    payload_type: type[BaseModel],
    raw_payload: dict[str, object] | BaseModel,
    actor: str | None = None,
    *,
    payload_schema_ver: str | None = None,
) -> BaseModel:
    """Validate a raw payload dict (or existing BaseModel) against the actor's payload model.

    Wraps ``pydantic.ValidationError`` as
    :class:`~taskq.exceptions.PayloadValidationError` (non-retryable) so
    the retry classifier fails the job immediately instead of retrying
    a deterministic validation failure.

    Error details are sanitized via ``include_url=False,
    include_input=False`` to prevent attacker-controlled field values
    from being persisted to the jobs row or surfaced in the web admin.

    Args:
        payload_type: The actor's Pydantic payload model class.
        raw_payload: The raw ``dict[str, object]`` from the job row, or
            an existing ``BaseModel`` to re-validate against
            ``payload_type``.
        actor: The actor name, for error context.
        payload_schema_ver: The schema version attached to the payload being
            validated — for a dispatch-time failure this is the job row's
            stored ``payload_schema_ver``, so an error handler can tell a
            row that predates a payload migration apart from a malformed
            caller payload. Defaults to the version being validated against
            today (:data:`CURRENT_PAYLOAD_SCHEMA_VER`), which is also the
            only version any stored row carries until a second version is
            introduced; the row-threading call sites land with that change.

    Returns:
        The validated ``BaseModel`` instance.

    Raises:
        PayloadValidationError: If validation fails.
    """
    try:
        return payload_type.model_validate(raw_payload)
    except ValidationError as exc:
        errs: list[dict[str, object]] = exc.errors(include_url=False, include_input=False)  # type: ignore[assignment]  # Why: pydantic v2 ErrorDetails is a TypedDict (subtype of dict[str, Any]); assignment to list[dict[str,object]] is safe at runtime but pyright cannot prove covariance
        raise PayloadValidationError(
            f"Payload validation failed for actor {actor!r}: {exc.title}",
            actor=actor,
            payload_schema_ver=(
                payload_schema_ver
                if payload_schema_ver is not None
                else str(CURRENT_PAYLOAD_SCHEMA_VER)
            ),
            validation_errors=errs,
        ) from exc

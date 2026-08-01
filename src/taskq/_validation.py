"""Shared payload validation helper."""

from pydantic import BaseModel, ValidationError

from taskq.exceptions import PayloadValidationError


def validate_actor_payload(
    payload_type: type[BaseModel],
    raw_payload: dict[str, object] | BaseModel,
    actor: str | None = None,
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
            validation_errors=errs,
        ) from exc

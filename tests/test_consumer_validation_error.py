"""Tests proving pydantic.ValidationError from payload validation at dispatch
time is converted to PayloadValidationError (non-retryable) with sanitized,
operator-safe diagnostics.

The conversion is the shared ``taskq.validate_actor_payload`` helper — the
exact call both dispatch paths make (``dispatch_one_job`` in
``taskq/worker/dispatch.py`` and ``consume_one_job``'s pre-acquire fallback
in ``taskq/worker/_consumer.py``). It follows the branch's sanitization
contract: neither the raised message nor the attached ``validation_errors``
embed the raw payload or its input values (a raw pydantic
``ValidationError`` repr embeds ``input_value=...``, and the
pre-sanitization message also rendered the whole payload dict), because the
message is persisted to the jobs row and surfaced in the web admin via
``error_message``. The diagnostics that ARE preserved: the actor name and
field-level context (loc/type/msg) — enough to identify which field failed
without seeing its value.

Adjacent contracts owned elsewhere: the terminal-write routing of the
converted error (error_class='PayloadValidationError') is pinned in
tests/test_dispatch_one_job.py and tests/test_in_memory_dispatch.py; the
public-export identity of the sanitized implementation in
tests/test_exceptions.py; the enqueue-time twin of this conversion in
tests/test_enqueue_coverage.py.

A leaked raw ``pydantic.ValidationError`` can never cause a retry storm:
the retry classifier fails it non-retryably with
error_class='PayloadValidationError' (defense in depth), separate from the
isinstance-based PayloadValidationError check.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from pydantic import BaseModel, ValidationError

from taskq import actor, validate_actor_payload
from taskq._ids import new_uuid
from taskq.context import JobContext
from taskq.exceptions import PayloadValidationError
from taskq.retry import Fail, RetryClassifier, RetryPolicy
from taskq.testing.actor import FakeBackend, as_backend, default_actor_config
from taskq.testing.clock import FakeClock
from taskq.testing.jobs import make_job_row
from taskq.worker._consumer import consume_one_job

_START = datetime(2025, 1, 1, tzinfo=UTC)
_WORKER_ID = new_uuid()
_CANARY = "TOP-SECRET-canary-c4d5e6"


class _SimplePayload(BaseModel):
    run_id: str
    batch_id: str


@actor(name="test_validation_error_actor")
async def _test_actor(_payload: _SimplePayload) -> None:
    pass


# ── pydantic.ValidationError is NOT PayloadValidationError ──────────────


class TestPydanticValidationErrorNotCaught:
    """Prove that pydantic.ValidationError leaking from model_validate is
    NOT caught by the retry classifier's PayloadValidationError check."""

    def test_pydantic_validation_error_is_not_payload_validation_error(self) -> None:
        """pydantic.ValidationError is a different class from
        PayloadValidationError — the classifier's isinstance check does not
        catch it."""
        assert not issubclass(ValidationError, PayloadValidationError)
        assert not issubclass(PayloadValidationError, ValidationError)

    def test_classifier_fails_pydantic_validation_error(self) -> None:
        """RetryClassifier.classify treats pydantic.ValidationError as
        non-retryable (Fail with error_class='PayloadValidationError'),
        even though it's a different class from PayloadValidationError.

        This is the defense-in-depth safety net: no matter where a raw
        pydantic.ValidationError leaks from, it can never cause a retry
        storm.
        """
        policy = RetryPolicy(max_attempts=50, base=timedelta(seconds=1))

        with pytest.raises(ValidationError) as exc_info:
            _SimplePayload.model_validate({"run_id": 123})
        decision = RetryClassifier.classify(
            policy=policy,
            non_retryable_exceptions=(),
            exception=exc_info.value,
            attempt=1,
        )
        assert isinstance(decision, Fail), (
            f"pydantic.ValidationError must be non-retryable in the classifier, got {decision!r}"
        )
        assert decision.error_class == "PayloadValidationError"
        assert decision.retryable is False


# ── PayloadValidationError IS non-retryable ─────────────────────────────


class TestPayloadValidationErrorNonRetryable:
    """PayloadValidationError is correctly non-retryable in the classifier."""

    def test_classifier_fails_payload_validation_error(self) -> None:
        """RetryClassifier.classify returns Fail for PayloadValidationError
        regardless of retry policy."""
        policy = RetryPolicy(max_attempts=50, base=timedelta(seconds=1))

        decision = RetryClassifier.classify(
            policy=policy,
            non_retryable_exceptions=(),
            exception=PayloadValidationError("bad payload"),
            attempt=1,
        )

        assert isinstance(decision, Fail)
        assert decision.error_class == "PayloadValidationError"
        assert decision.retryable is False


# ── The real conversion: sanitized, structured, non-retryable ───────────


class TestDispatchConvertsValidationError:
    """The dispatch-time conversion is ``taskq.validate_actor_payload`` —
    the exact call ``dispatch_one_job`` and ``consume_one_job`` make. A
    malformed payload raises PayloadValidationError whose diagnostics are
    sanitized: the raw payload and its input values appear NOWHERE (not in
    the message, not in validation_errors), while the actor name and
    field-level context are preserved.
    """

    def test_conversion_message_is_sanitized_not_raw(self) -> None:
        """Driving the real conversion with a canary-bearing invalid
        payload: the correct exception type is raised, the actor name is
        preserved, and the payload's values ("12345", the canary) appear in
        neither the message nor the attached validation_errors. The raw
        payload dict is not rendered either — the pre-sanitization
        "Raw payload: {payload}" shape is gone."""
        bad_payload = {"run_id": 12345, "batch_id": {"secret": _CANARY}}

        with pytest.raises(PayloadValidationError) as exc_info:
            validate_actor_payload(_test_actor.payload_type, bad_payload, _test_actor.name)

        exc = exc_info.value
        # Actor context IS preserved — the piece of caller text the
        # message is allowed to carry.
        assert _test_actor.name in str(exc)
        assert exc.actor == _test_actor.name
        # The raw payload and its input values are NOT:
        assert _CANARY not in str(exc)
        assert "12345" not in str(exc)
        assert "Raw payload" not in str(exc)
        assert all(_CANARY not in str(err) for err in exc.validation_errors)
        assert all("12345" not in str(err) for err in exc.validation_errors)

    def test_conversion_preserves_field_context_without_input_values(self) -> None:
        """The sanitized errors stay operator-debuggable: each carries
        loc/type/msg naming the failed field and the failure kind, but
        never the 'input' (field value) or 'url' keys an unsanitized
        ``exc.errors()`` embeds."""
        with pytest.raises(PayloadValidationError) as exc_info:
            validate_actor_payload(
                _test_actor.payload_type,
                {"run_id": 12345, "batch_id": None},
                _test_actor.name,
            )

        errors = exc_info.value.validation_errors
        assert len(errors) == 2
        assert {err["loc"] for err in errors} == {("run_id",), ("batch_id",)}
        for err in errors:
            assert "type" in err
            assert "msg" in err
            assert "input" not in err
            assert "url" not in err

    def test_conversion_chains_the_original_validation_error(self) -> None:
        """The PayloadValidationError wraps the original pydantic
        ValidationError via ``__cause__`` (raise ... from exc), so the full
        unsanitized detail stays reachable for a handler that deliberately
        inspects the cause."""
        with pytest.raises(PayloadValidationError) as exc_info:
            validate_actor_payload(_test_actor.payload_type, {"run_id": 12345}, _test_actor.name)

        assert isinstance(exc_info.value.__cause__, ValidationError)

    def test_real_converted_error_is_non_retryable(self) -> None:
        """The exception the real conversion raises is classified
        non-retryable (Fail, error_class='PayloadValidationError')
        regardless of the actor's retry policy — a deterministic validation
        failure must not burn retry attempts."""
        with pytest.raises(PayloadValidationError) as exc_info:
            validate_actor_payload(_test_actor.payload_type, {"run_id": 12345}, _test_actor.name)

        decision = RetryClassifier.classify(
            policy=RetryPolicy(max_attempts=50, base=timedelta(seconds=1)),
            non_retryable_exceptions=(),
            exception=exc_info.value,
            attempt=1,
        )

        assert isinstance(decision, Fail)
        assert decision.error_class == "PayloadValidationError"
        assert decision.retryable is False


# ── The consumer path drives the sanitized conversion ───────────────────


class TestConsumerPathUsesTheSanitizedConversion:
    """``consume_one_job``'s pre-acquire fallback (``validated_payload=None``)
    drives the SAME sanitized ``validate_actor_payload`` conversion — this
    pins the wiring, not just the helper: replacing that call with an
    inline ``model_validate`` plus a message embedding the raw payload
    would fail here.
    """

    async def test_consumer_propagates_sanitized_payload_validation_error(self) -> None:
        """A malformed stored payload makes consume_one_job propagate
        PayloadValidationError before the actor body runs, and the
        propagated exception carries no raw payload values."""
        job = make_job_row(
            actor=_test_actor.name,
            payload={"run_id": 12345, "batch_id": {"secret": _CANARY}},
        )

        async def never_runs(_job: object, _ctx: JobContext[BaseModel]) -> object:
            raise AssertionError("actor body must not run on validation failure")

        with pytest.raises(PayloadValidationError) as exc_info:
            await consume_one_job(
                as_backend(FakeBackend()),
                job,
                _WORKER_ID,
                run_actor=never_runs,
                actor_config=default_actor_config(),
                payload_type=_test_actor.payload_type,
                clock=FakeClock(_START),
            )

        exc = exc_info.value
        assert exc.actor == _test_actor.name
        assert _CANARY not in str(exc)
        assert "12345" not in str(exc)
        assert all(_CANARY not in str(err) for err in exc.validation_errors)

"""Pin: PayloadValidationError.payload_schema_ver is a documented field
that every production raise site must populate -- it carries the
payload's schema version so an adopter can distinguish "this row
predates a payload migration" from "this caller sent garbage".

``docs/guides/jobs-clients.md``'s error-handling table (the row for
``PayloadValidationError``, in the "Error handling" section) lists the
exception's fields as "actor, payload_schema_ver, validation_errors" --
presented as three fields an adopter can branch on. The constructor
(``taskq/exceptions.py::PayloadValidationError.__init__``, around line
367) accepts ``payload_schema_ver: str | None = None``, and the JobRow
column it shares a name with
(``payload_schema_ver int NOT NULL DEFAULT 1`` --
``src/taskq/migrations/01.00.00_01_pre_initial.sql:78,347``) is real,
threaded, and stored on every row.

There are exactly four call sites across the whole source tree that
construct ``PayloadValidationError`` (``grep -rn
"raise PayloadValidationError("`` over ``src/taskq``, plus the two
construct-without-raise helpers the batch paths return from):

    1. taskq/_validation.py            (validate_actor_payload --
       the shared helper both dispatch paths call)
    2. taskq/backend/_records.py       (_nul_item_payload_error --
       per-item NUL-byte rejection in batch jsonb serialization)
    3. taskq/ratelimit/registry.py     (inside _resolve_key_fn_arg --
       KeyedRateLimitRef / KeyedReservationRef cross-model payload
       re-validation)
    4. taskq/client/_jobs.py           (_item_payload_error --
       streaming/chunked batch enqueue validation)

What each site passes, and why (all pinned below):

    * Sites 1, 3 and 4 have no older row in scope -- they validate
      against the actor's CURRENTLY declared payload_type, so they pass
      the version being validated against
      (``str(CURRENT_PAYLOAD_SCHEMA_VER)``), which is also the version
      an enqueue-time row is about to be stamped with.
    * Site 2 fires before any row exists (a pre-INSERT jsonb-encoding
      guard), so None is correct there -- pinned as a passing test so a
      future maintainer does not mistake the legitimate None for the
      gap the other sites had.
    * The two WORKER-path call sites of site 1's helper hold the job
      row and must pass the row's STORED version instead of the
      current-version default: ``worker/_consumer.py``'s pre-acquire
      fallback and ``worker/dispatch.py``'s pre-scope validation. Those
      pins live beside the harnesses that already drive those paths
      driver-free -- ``TestConsumerThreadsStoredSchemaVer`` in
      tests/test_consumer_validation_error.py (the exception propagates
      out of consume_one_job) and
      ``test_dispatch_threads_the_rows_stored_schema_ver`` in
      tests/test_dispatch_one_job.py (dispatch_one_job routes the
      failure into the terminal write, so the wiring is pinned with a
      delegate spy).

This matters because the docs present the field as part of the
diagnostic contract for a mid-flight payload schema change (a scenario
this repo's own footgun index does not otherwise cover): an adopter who
writes ``except PayloadValidationError as exc: if exc.payload_schema_ver
== OLD_VERSION: ...`` to distinguish "row predates this schema" from
"caller sent garbage" must get a truthful value on every path.

No vendor precedent search applies: this is not a missing capability
relative to another queue library (River/Oban/Sidekiq have no
Pydantic-model payload-versioning concept at all -- see
tests/test_register_stub_payload_type_fidelity.py's docstring for that
same point argued in full). This is argued from internal consistency: a
field documented as part of an exception's public contract
(jobs-clients.md's error table) must be populated by every path that
contract's callers can reach, or the docs overstate what the field is
for.
"""

from __future__ import annotations

import pytest
from pydantic import BaseModel, ValidationError

from taskq._validation import validate_actor_payload
from taskq.backend._records import (
    _nul_item_payload_error,  # pyright: ignore[reportPrivateUsage]  # Why: pinning the internal raise site directly by design -- this is a whitebox test of every PayloadValidationError constructor call, not a behavioral/blackbox test.
)
from taskq.client._jobs import (
    _item_payload_error,  # pyright: ignore[reportPrivateUsage]  # Why: same whitebox rationale as above.
)
from taskq.exceptions import PayloadValidationError
from taskq.ratelimit.refs import KeyedRateLimitRef
from taskq.ratelimit.registry import RateLimitRegistry


class _Payload(BaseModel):
    value: int


class _OtherPayload(BaseModel):
    other: str


def test_validate_actor_payload_populates_schema_ver() -> None:
    """Call site 1: taskq/_validation.py:41 -- the shared helper both
    dispatch paths (dispatch_one_job, consume_one_job's pre-acquire
    fallback) call on every real payload validation, enqueue and
    dispatch alike. This is the site the mid-flight schema-drift
    scenario actually hits (an old row's stored payload no longer
    satisfies a newly-required field): it has direct access to the job
    row's stored ``payload_schema_ver`` and is where an adopter's
    error handler most needs it populated.
    """
    with pytest.raises(PayloadValidationError) as exc_info:
        validate_actor_payload(_Payload, {"value": "not-an-int-but-also-missing"}, "some_actor")

    assert exc_info.value.payload_schema_ver is not None, (
        "validate_actor_payload's PayloadValidationError leaves "
        "payload_schema_ver=None. docs/guides/jobs-clients.md's error "
        "table documents this field as part of the exception's public "
        "contract; an adopter branching on it to tell a pre-migration "
        "row apart from a malformed caller payload gets no signal. Wire "
        "the actor's/job's payload_schema_ver through this raise site."
    )


def test_item_payload_error_populates_schema_ver() -> None:
    """Call site 4: taskq/client/_jobs.py:121 -- _item_payload_error,
    the streaming/chunked batch enqueue path's per-item annotation.
    This runs at enqueue time against the actor's currently-declared
    payload_type, so the schema version being validated against is
    known at the call site and should be threaded into the exception.
    """
    try:
        _Payload.model_validate({"value": "not-an-int"})
        pytest.fail("expected ValidationError from the deliberately bad payload")
    except ValidationError as pyd_exc:
        exc = _item_payload_error(0, "some_actor", pyd_exc)

    assert isinstance(exc, PayloadValidationError)
    assert exc.payload_schema_ver is not None, (
        "_item_payload_error's PayloadValidationError leaves "
        "payload_schema_ver=None even though the batch enqueue path "
        "knows which actor and payload_type it validated against. "
        "Thread it through so batch callers get the same diagnostic "
        "the docs promise for single-item enqueue."
    )


def test_keyed_ref_cross_model_revalidation_populates_schema_ver() -> None:
    """Call site 3: taskq/ratelimit/registry.py:645 (inside
    ``_resolve_key_fn_arg``) -- the keyed rate-limit/reservation ref
    path re-validates a payload against a DIFFERENT model than the one
    it was already validated against (cross-actor keyed bucket
    derivation), and wraps the resulting ValidationError the same way.

    ``_resolve_key_fn_arg``'s own docstring (ratelimit/registry.py
    ~600-635) names exactly this: "Different BaseModel type: re-validate
    via ref.payload_type.model_validate(payload.model_dump(by_alias=True))"
    -- pinned by the project's own
    test_resolve_keyed_ref_wrong_model_type_raises_validation_error.
    This test drives the same conversion and asserts the desired
    behaviour: the raised exception carries a populated
    payload_schema_ver, not None.
    """
    registry = RateLimitRegistry()

    ref = KeyedRateLimitRef.typed(
        _Payload,
        base_name="cross-model-probe",
        key_fn=lambda p: str(p.value),
        capacity=10,
        refill_per_second=1.0,
    )

    # _OtherPayload has no `value` field _Payload requires -- the
    # dump -> validate round-trip the docstring describes fails.
    source = _OtherPayload(other="x")

    with pytest.raises(PayloadValidationError) as exc_info:
        registry._resolve_key_fn_arg(ref, source)  # pyright: ignore[reportPrivateUsage]  # Why: whitebox call into the private conversion helper with a deliberately mismatched model to hit the cross-model except-ValidationError branch (registry.py:645).

    assert exc_info.value.payload_schema_ver is not None, (
        "The keyed-ref cross-model PayloadValidationError leaves "
        "payload_schema_ver=None. Thread ref.payload_type's known "
        "schema context through this raise site too."
    )


def test_docs_implied_contract_is_usable_end_to_end() -> None:
    """The concrete adopter footgun this whole file pins: branching on
    payload_schema_ver to tell 'this row predates the schema change'
    apart from 'this caller sent garbage' must be possible, because
    the docs present the field as existing for exactly that purpose.
    Both calls below go through the shared helper (the dispatch paths'
    site), which populates the field on every failure.
    """
    old_row_style_failure = None
    garbage_caller_failure = None

    try:
        validate_actor_payload(_Payload, {"value": "predates-the-new-required-field"}, "actor_a")
    except PayloadValidationError as exc:
        old_row_style_failure = exc.payload_schema_ver

    try:
        validate_actor_payload(_Payload, {"totally": "wrong-shape"}, "actor_a")
    except PayloadValidationError as exc:
        garbage_caller_failure = exc.payload_schema_ver

    assert old_row_style_failure is not None or garbage_caller_failure is not None, (
        "Both real PayloadValidationError instances have "
        "payload_schema_ver=None -- there is no way for adopter code to "
        "distinguish a pre-migration row from a malformed caller payload "
        "using the field docs/guides/jobs-clients.md's error table "
        "presents for exactly that purpose. Populate the field at at "
        "least one raise site (validate_actor_payload is the one that "
        "matters most, since it is what dispatch-time schema drift "
        "actually hits), or remove the field from the documented "
        "contract and the constructor's public surface if it is never "
        "going to be wired up."
    )


def test_nul_item_payload_error_has_no_row_context_documented_as_such() -> None:
    """Call site 2: taskq/backend/_records.py:83 --
    _nul_item_payload_error, the per-item NUL-byte rejection guard batch
    enqueue serialization uses BEFORE any row exists (it fires during
    jsonb-encoding, pre-INSERT). Unlike the other three sites, there is
    no stored payload_schema_ver to read here -- this is a
    first-principles exception, not a parity gap: a field cannot carry
    a job's schema version before the job has one.

    This is pinned as a passing (not red) test precisely to document
    that distinction for a future maintainer wiring up the other three
    sites: do not force a value here, and do not let this site's
    legitimate None be mistaken for the same bug the other three
    sites have.
    """
    exc = _nul_item_payload_error(idx=0, field="payload", actor="some_actor")

    assert isinstance(exc, PayloadValidationError)
    assert exc.payload_schema_ver is None, (
        "_nul_item_payload_error fires before any job row (or its "
        "payload_schema_ver) exists -- this is the one call site where "
        "None is correct, not a gap. If this now fails because the "
        "call site changed to fire post-INSERT, reconsider whether it "
        "should populate the field like the other three sites."
    )

# KeyedRateLimitRef / KeyedReservationRef: Typed `key_fn` Payload

**Date:** 2026-07-29
**Status:** Draft
**Issue:** #60

## Goal

`KeyedRateLimitRef.key_fn` and `KeyedReservationRef.key_fn` currently receive the raw `dict[str, object]` from the job row — the pre-validation wire form — rather than the actor's validated Pydantic model. This means Pydantic defaults, aliases, and validation are not applied before `key_fn` runs, causing `key_fn` and the handler to disagree about the same field, coupling routing keys to serialization names rather than model attributes, and surfacing field-shape failures as limiter faults rather than payload-validation errors.

This spec makes `key_fn` **always** receive the validated Pydantic model. This is a **breaking change** — intentional and correct for a pre-1.0 library heading to 1.0. The downstream consumers (warden, cennan, aacrtool) are waiting on these features and have no shipped code that needs preserving. Existing `key_fn` implementations that access raw dict keys (`p["tenantId"]`) must be updated to access model attributes (`p.tenant_id`).

The design:
- `payload_type: type[BaseModel]` becomes a **required** field on both refs (not optional, not `None` default).
- `key_fn` is typed as `Callable[[BaseModel], str]` — it always receives the validated model.
- A `.typed()` classmethod provides caller-side compile-time type safety (generic over `P: BaseModel`).
- The registry validates any incoming payload (dict or BaseModel) against `ref.payload_type` before calling `key_fn`, with an `isinstance` check for zero-cost pass-through on same-type models.
- `_consumer.py` **unconditionally** passes `validated_payload` (the BaseModel) to `acquire_for_actor` — no gating, no conditional, no raw-dict fallback path.

## Non-goals

- **Moving payload validation before admission control globally.** The dispatch path in `dispatch_one_job` already validates the payload at line 180 *before* `consume_one_job` is called. The issue is not ordering of validation vs. admission control at the dispatch level — it is that `_consumer.py:310` passes `job.payload` (the raw dict) to `acquire_for_actor` instead of the already-validated `validated_payload` (a `BaseModel`) that was passed in as a parameter. This spec fixes the variable passed; it does not reorder validation relative to admission control.
- **Preserving backward compatibility for untyped `key_fn` implementations.** This is a **breaking change**. Existing `key_fn` implementations that access raw dict keys like `p["providerId"]` must be updated to access model attributes like `p.provider_id`. TaskQ is pre-1.0, heading to 1.0.0, and the downstream consumers are waiting on these features — they don't have shipped code that needs preserving. The stronger, typesafe design is strictly better; we break and do it right.
- **Changing the `@actor` decorator signature.** The `rate_limits` and `reservations` parameters on `@actor` already accept `list[str | KeyedRateLimitRef]` and `list[str | KeyedReservationRef]`. The refs themselves gain a required field; the decorator does not change.
- **Adding `payload_type` enforcement at decoration time.** The DI validation phase at startup checks that static names are registered; it does not (and should not) inspect `key_fn` signatures or `payload_type` fields. Runtime validation in the registry is the enforcement point.

## Architecture Overview

### Current dispatch flow (the seam)

```
dispatch_one_job (dispatch.py:180)
  │
  ├── validated_payload = actor_ref.payload_type.model_validate(job.payload)  ← payload IS validated here
  │
  └── consume_one_job(..., validated_payload=validated_payload, ...)  (dispatch.py:280)
        │
        ├── acquire_for_actor(..., payload=job.payload, ...)  ← RAW dict, not validated_payload!  (_consumer.py:310)
        │     │
        │     └── _resolve_rate_limit_name(ref, payload=job.payload, ...)  (registry.py:618)
        │           │
        │           └── ref.key_fn(payload)  ← key_fn sees raw dict
        │
        └── validated_payload = validated_payload if validated_payload is not None
                              else payload_type.model_validate(job.payload)  ← used for actor ctx  (_consumer.py:357-361)
```

The validated model is constructed in `dispatch_one_job` and passed to `consume_one_job` as the `validated_payload` parameter, but `consume_one_job` passes `job.payload` (the raw dict) to `acquire_for_actor` instead of the already-validated model. The fix:

1. **Move the `validated_payload` fallback before `acquire_for_actor`** so the model is always available when rate limits are acquired. This also means a `ValidationError` from an invalid payload surfaces *before* a rate-limit token is consumed — the correct behavior.
2. **Always pass `validated_payload` (the BaseModel) to `acquire_for_actor`** — unconditionally, no gating.
3. **Make `payload_type` a required field on both refs** so the registry can validate/convert any incoming payload and always pass the validated model to `key_fn`.

### Proposed dispatch flow (after fix)

```
dispatch_one_job (dispatch.py:180)
  │
  ├── validated_payload = actor_ref.payload_type.model_validate(job.payload)
  │
  └── consume_one_job(..., validated_payload=validated_payload, ...)
        │
        ├── if validated_payload is None:                                    ← moved BEFORE acquire
        │     validated_payload = payload_type.model_validate(job.payload)
        │
        ├── acquire_for_actor(..., payload=validated_payload, ...)  ← ALWAYS the model  (_consumer.py:310)
        │     │
        │     └── _resolve_rate_limit_name(ref, payload=validated_payload, ...)  (registry.py:618)
        │           │
        │           ├── isinstance(payload, ref.payload_type):
        │           │     → key_fn(payload)  ← same model type, zero-cost pass-through
        │           ├── isinstance(payload, BaseModel) but wrong type:
        │           │     → model = ref.payload_type.model_validate(payload.model_dump())
        │           │     → key_fn(model)  ← re-validate against ref's model
        │           └── payload is dict → model = ref.payload_type.model_validate(dict)
        │                                 → key_fn(model)  ← validated model
        │
        └── validated_payload (reused for actor ctx — no double validation)
```

**Model-type semantics (H1):** The actor's `payload_type` and a ref's `payload_type` are independent declarations; nothing enforces they are the same model. The registry handles this explicitly:

- **Same type** (`isinstance(payload, ref.payload_type)` is `True`): zero-cost pass-through — the model is already validated against the correct type.
- **Different `BaseModel` type** (mismatch): the registry re-validates via `ref.payload_type.model_validate(payload.model_dump())`. This converts the actor's model into the ref's declared model, applying the ref's aliases, defaults, and validators. A `ValidationError` from this conversion surfaces as a payload error, not a limiter fault.
- **Raw dict**: always validated via `ref.payload_type.model_validate(dict)` regardless of source. This path is for direct callers of `acquire_for_actor` (tests, non-dispatch callers); the production dispatch path always passes a `BaseModel`.

There is **no untyped path**. `key_fn` always receives the validated model. There is no `model_dump()` fallback, no raw-dict pass-through to `key_fn`.

### File structure

```
src/taskq/ratelimit/refs.py          — Make payload_type required; change key_fn type to Callable[[BaseModel], str];
                                        add .typed() classmethod to both refs;
                                        update module/class docstrings (dict→model examples)
src/taskq/ratelimit/registry.py      — Update _resolve_*_name to always validate/convert payload to model;
                                        update acquire_for_actor signature: dict | BaseModel | None
src/taskq/worker/_consumer.py        — Move validated_payload fallback before acquire_for_actor;
                                        always pass validated_payload (BaseModel) to acquire_for_actor

tests/test_ratelimit_keyed_rate_limits.py  — Migrate _rate_limit_ref helper to .typed();
                                              add typed key_fn tests; remove untyped-path tests
tests/test_ratelimit_keyed_refs.py         — Migrate _keyed_ref helper to .typed();
                                              add typed key_fn tests; remove untyped-path tests
tests/test_keyed_reservation_hardening.py  — Migrate _keyed_ref helper to .typed()
tests/test_ratelimit_composition.py        — Migrate ref constructions to .typed()
tests/test_ratelimit_refs.py               — Add .typed() classmethod tests; required payload_type tests
tests/test_di_validate.py                  — Migrate ref constructions to .typed()
tests/test_worker_di_bootstrap.py          — Migrate ref construction to .typed()
tests/test_actor.py                        — Migrate ref construction to .typed()
tests/test_queue_concurrency_cap.py        — Migrate ref constructions to .typed()
tests/web_admin/test_ops.py               — Migrate ref construction to .typed()
tests/test_ratelimit_keyed_rate_limits_pg.py — Migrate ref constructions to .typed()
tests/test_ratelimit_keyed_refs_pg.py       — Migrate ref constructions to .typed()
tests/e2e/actors.py                        — Migrate deliver_tenant_webhook to .typed();
                                              add typed-keyed-rate-limit actor with aliased payload
tests/e2e/worker_entry.py                  — Register new typed actor
tests/e2e/test_keyed_rate_limit.py         — Add e2e test for typed key_fn with aliases

docs/guides/rate-limiting.md        — Update key_fn documentation (model, not dict)
docs/architecture.md                — Update dispatch integration section
```

## API Surface

### `KeyedRateLimitRef` — required `payload_type`, typed `key_fn`, `.typed()` classmethod

```python
from collections.abc import Callable
from datetime import timedelta
from pydantic import BaseModel, ConfigDict, field_validator
from taskq.backend._protocol import RateLimitBackend

class KeyedRateLimitRef(BaseModel):
    """Reference to a per-key token bucket, derived from the payload.

    ``key_fn`` receives the actor's validated Pydantic model (with defaults,
    aliases, and validation applied) and must return a non-empty string.
    The ``payload_type`` field declares the model class the registry
    validates the payload against before calling ``key_fn``.

    For type-safe construction with compile-time checking of ``key_fn``
    against the payload model, use the :meth:`typed` classmethod::

        KeyedRateLimitRef.typed(
            MyPayload,
            base_name="api-per-tenant",
            key_fn=lambda p: p.tenant_id,  # p is MyPayload — typed, defaults applied
            capacity=10,
            refill_per_second=1.0,
        )

    Direct construction is also possible but does not provide compile-time
    type checking of the ``key_fn`` lambda body (``p`` is typed as
    ``BaseModel``)::

        KeyedRateLimitRef(
            base_name="api-per-tenant",
            key_fn=lambda p: p.tenant_id,
            payload_type=MyPayload,  # required
            capacity=10,
            refill_per_second=1.0,
        )
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    base_name: str
    key_fn: Callable[[BaseModel], str]
    payload_type: type[BaseModel]  # required — no default, no | None
    capacity: float
    refill_per_second: float
    backend: RateLimitBackend = "redis"

    @classmethod
    def typed[P: BaseModel](
        cls,
        payload_type: type[P],
        *,
        base_name: str,
        key_fn: Callable[[P], str],
        capacity: float,
        refill_per_second: float,
        backend: RateLimitBackend = "redis",
    ) -> KeyedRateLimitRef:
        """Create a ref whose ``key_fn`` receives the validated model ``P``.

        The ``payload_type`` is stored on the ref so the registry can
        validate the payload against it before calling ``key_fn``. At
        runtime, the registry passes the validated model to ``key_fn``;
        the ``key_fn`` callable never sees a raw dict.

        Args:
            payload_type: The Pydantic model class to validate the
                payload against before calling ``key_fn``.
            base_name: Namespace for derived buckets.
            key_fn: Function that receives the validated model and
                returns a non-empty string key.
            capacity: Token bucket capacity.
            refill_per_second: Token refill rate.
            backend: Storage backend (default ``"redis"``).
        """
        return cls(
            base_name=base_name,
            key_fn=key_fn,  # type: ignore[arg-type]  # Why: contravariance — Callable[[P], str] is not assignable to Callable[[BaseModel], str] in the type system, but at runtime the registry only passes the validated model P (verified by isinstance check against ref.payload_type).
            payload_type=payload_type,
            capacity=capacity,
            refill_per_second=refill_per_second,
            backend=backend,
        )

    # ... existing field validators unchanged ...
```

### `KeyedReservationRef` — same pattern

```python
class KeyedReservationRef(BaseModel):
    """Reference to a per-key concurrency reservation, derived from the payload.

    ``key_fn`` receives the actor's validated Pydantic model and must
    return a non-empty string. The ``payload_type`` field declares the
    model class the registry validates the payload against.

    See :class:`KeyedRateLimitRef.typed` for the typed construction pattern.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    base_name: str
    key_fn: Callable[[BaseModel], str]
    payload_type: type[BaseModel]  # required — no default, no | None
    slots: int
    lease: timedelta

    @classmethod
    def typed[P: BaseModel](
        cls,
        payload_type: type[P],
        *,
        base_name: str,
        key_fn: Callable[[P], str],
        slots: int,
        lease: timedelta,
    ) -> KeyedReservationRef:
        """Create a ref whose ``key_fn`` receives the validated model ``P``.

        See :meth:`KeyedRateLimitRef.typed` for the full rationale.
        """
        return cls(
            base_name=base_name,
            key_fn=key_fn,  # type: ignore[arg-type]  # Why: same as KeyedRateLimitRef.typed
            payload_type=payload_type,
            slots=slots,
            lease=lease,
        )

    # ... existing field validators unchanged ...
```

### `RateLimitRegistry.acquire_for_actor` — updated signature

```python
async def acquire_for_actor(
    self,
    rate_limits: Sequence["str | KeyedRateLimitRef"],
    reservations: Sequence["str | KeyedReservationRef"],
    *,
    job_id: "UUID",
    worker_id: "UUID",
    payload: dict[str, object] | BaseModel | None = None,  # ← was dict[str, object] | None
    redis_client: "redis_async.Redis | None" = None,
    pg_pool: "asyncpg.Pool | None" = None,
    clock: "Clock | None" = None,
    settings: "WorkerSettings | None" = None,
) -> list[AcquiredResource]:
```

The `payload` parameter accepts `dict[str, object]` for direct callers (tests, non-dispatch callers) — the registry validates the dict against `ref.payload_type` before calling `key_fn`. On the production dispatch path, `_consumer.py` always passes a `BaseModel`.

### `RateLimitRegistry._resolve_rate_limit_name` — updated signature and logic

```python
async def _resolve_rate_limit_name(
    self,
    ref: "str | KeyedRateLimitRef",
    payload: dict[str, object] | BaseModel | None,  # ← was dict[str, object] | None
    *,
    settings: "WorkerSettings | None",
    pg_pool: "asyncpg.Pool | None" = None,
) -> str:
    # ... existing isinstance(ref, str) check unchanged ...

    if payload is None:
        raise ValueError(...)

    # Always convert to validated model — key_fn always receives the model.
    # There is no untyped path, no model_dump() fallback.
    if isinstance(payload, ref.payload_type):
        key_fn_arg: BaseModel = payload  # same model type — zero-cost pass-through
    elif isinstance(payload, BaseModel):
        # Wrong model type — re-validate against the ref's declared model.
        # This handles refs that declare a narrowed/related model different
        # from the actor's payload_type. A ValidationError from this
        # conversion surfaces as a payload error, not a limiter fault.
        key_fn_arg = ref.payload_type.model_validate(payload.model_dump())
    else:
        # Raw dict — validate against the ref's declared model
        key_fn_arg = ref.payload_type.model_validate(payload)

    # Build the payload dict for error messages
    error_payload = (
        payload
        if isinstance(payload, dict)
        else payload.model_dump() if isinstance(payload, BaseModel) else None
    )

    key = self._validate_keyed_key(
        ref.key_fn(key_fn_arg),
        f"KeyedRateLimitRef(base_name={ref.base_name!r})",
        error_payload,
    )
    # ... rest unchanged ...
```

### `_consumer.py` — always pass `validated_payload` (unconditional)

```python
# _consumer.py, move the validated_payload fallback BEFORE acquire_for_actor
# and change payload=job.payload to payload=validated_payload.
#
# BEFORE (current code):
#   if _needs_acquire and rate_limit_registry is not None:
#       try:
#           acquired = await rate_limit_registry.acquire_for_actor(
#               ...,
#               payload=job.payload,        ← RAW dict
#               ...,
#           )
#   ...
#   try:
#       validated_payload = (
#           validated_payload
#           if validated_payload is not None
#           else payload_type.model_validate(job.payload)
#       )
#
# AFTER (fixed code):
#   # Ensure the validated model is available before rate-limit acquisition.
#   # On the dispatch path, validated_payload is already set (dispatch_one_job
#   # validates at line 180). For direct callers, validate here. A
#   # ValidationError from an invalid payload now surfaces BEFORE a
#   # rate-limit token is consumed — the correct behavior.
#   if validated_payload is None:
#       validated_payload = payload_type.model_validate(job.payload)
#
#   if _needs_acquire and rate_limit_registry is not None:
#       try:
#           acquired = await rate_limit_registry.acquire_for_actor(
#               ...,
#               payload=validated_payload,  ← ALWAYS the model
#               ...,
#           )
#   ...
#   # validated_payload is already set — the later fallback is removed
```

No gating. No `any(r.payload_type is not None)` check. No conditional. `validated_payload` is always a `BaseModel` when it reaches `acquire_for_actor`. This is a **breaking change** for any `key_fn` that accesses raw dict keys — which is the point. The issue itself says "What I'd rather have is `key_fn` receiving the validated model."

## Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `key_fn` always receive the validated Pydantic model. This is a breaking change: `payload_type` becomes required, `key_fn` accesses model attributes instead of dict keys, and `_consumer.py` always passes the validated model.

**Architecture:** Make `payload_type` a required field on `KeyedRateLimitRef` and `KeyedReservationRef`, change `key_fn` to `Callable[[BaseModel], str]`, add a `.typed()` classmethod for type-safe construction, update the registry to always validate/convert the payload to the model (with `isinstance` check for zero-cost pass-through on same-type models), and fix `_consumer.py` to unconditionally pass `validated_payload` — moving the validation fallback before `acquire_for_actor` so the model is always available.

**Tech Stack:** Python 3.12+, Pydantic 2.13+, asyncpg, structlog

---

### Task 1: Make `payload_type` required, add `.typed()` to `KeyedRateLimitRef`; migrate existing constructions

**Files:**
- Modify: `src/taskq/ratelimit/refs.py`
- Modify: `tests/test_ratelimit_keyed_rate_limits.py` (migrate `_rate_limit_ref` helper + all `KeyedRateLimitRef` constructions)
- Modify: `tests/test_ratelimit_keyed_rate_limits_pg.py`
- Modify: `tests/test_ratelimit_composition.py`
- Modify: `tests/test_ratelimit_refs.py`
- Modify: `tests/test_di_validate.py`
- Modify: `tests/test_worker_di_bootstrap.py`
- Modify: `tests/test_queue_concurrency_cap.py`
- Modify: `tests/web_admin/test_ops.py`
- Modify: `tests/e2e/actors.py` (`deliver_tenant_webhook` actor)
- Test: `tests/test_ratelimit_refs.py`

- [ ] **Step 1: Write failing tests for `KeyedRateLimitRef.typed()` and required `payload_type`**

Add to `tests/test_ratelimit_refs.py`:

> **Import note (L1):** The test file currently imports only `ValidationError` from pydantic (line 8). Add `BaseModel` and `Field` to the import: `from pydantic import BaseModel, Field, ValidationError`. The method-local `class MyPayload(BaseModel)` classes below require this import.

```python
class TestKeyedRateLimitRefTyped:
    def test_typed_classmethod_creates_ref_with_payload_type(self) -> None:
        """KeyedRateLimitRef.typed() stores payload_type and key_fn."""
        from taskq.ratelimit import KeyedRateLimitRef

        class MyPayload(BaseModel):
            tenant_id: str

        ref = KeyedRateLimitRef.typed(
            MyPayload,
            base_name="api-per-tenant",
            key_fn=lambda p: p.tenant_id,
            capacity=10.0,
            refill_per_second=1.0,
        )
        assert ref.payload_type is MyPayload
        assert ref.base_name == "api-per-tenant"
        assert ref.capacity == 10.0
        assert ref.refill_per_second == 1.0

    def test_payload_type_is_required(self) -> None:
        """Construction without payload_type raises ValidationError."""
        from pydantic import ValidationError
        from taskq.ratelimit import KeyedRateLimitRef

        with pytest.raises(ValidationError):
            KeyedRateLimitRef(  # pyright: ignore[reportCallIssue]
                base_name="api-per-tenant",
                key_fn=lambda p: str(p),
                capacity=10.0,
                refill_per_second=1.0,
                # payload_type missing — required
            )

    def test_typed_classmethod_key_fn_receives_model_not_dict(self) -> None:
        """key_fn from .typed() can access model attributes, not dict keys."""
        from taskq.ratelimit import KeyedRateLimitRef

        class MyPayload(BaseModel):
            tenant_id: str
            region: str = "us-east-1"  # default

        received: list[object] = []
        ref = KeyedRateLimitRef.typed(
            MyPayload,
            base_name="api-per-tenant",
            key_fn=lambda p: (received.append(p), p.tenant_id)[1],
            capacity=10.0,
            refill_per_second=1.0,
        )
        # Simulate what the registry does: validate dict → model → key_fn(model)
        model = MyPayload.model_validate({"tenant_id": "acme"})
        result = ref.key_fn(model)
        assert result == "acme"
        assert isinstance(received[0], MyPayload)
        assert received[0] == model  # type: ignore[union-attr]

    def test_typed_with_backend_memory(self) -> None:
        """KeyedRateLimitRef.typed() accepts backend parameter."""
        from taskq.ratelimit import KeyedRateLimitRef

        class MyPayload(BaseModel):
            tenant_id: str

        ref = KeyedRateLimitRef.typed(
            MyPayload,
            base_name="api-per-tenant",
            key_fn=lambda p: p.tenant_id,
            capacity=10.0,
            refill_per_second=1.0,
            backend="memory",
        )
        assert ref.backend == "memory"

    def test_typed_key_fn_with_default_field(self) -> None:
        """A field with a Pydantic default is populated on the model
        even if absent from the raw dict — key_fn can read it."""
        from taskq.ratelimit import KeyedRateLimitRef

        class MyPayload(BaseModel):
            tenant_id: str
            priority: str = "normal"  # default not in raw dict

        ref = KeyedRateLimitRef.typed(
            MyPayload,
            base_name="api-per-tenant",
            key_fn=lambda p: f"{p.tenant_id}:{p.priority}",
            capacity=10.0,
            refill_per_second=1.0,
        )
        # Raw dict has no "priority" key — default applies on model_validate
        model = MyPayload.model_validate({"tenant_id": "acme"})
        result = ref.key_fn(model)
        assert result == "acme:normal"

    def test_typed_key_fn_with_alias(self) -> None:
        """A field with Field(alias=...) maps the wire name to the model
        attribute — key_fn uses the attribute, not the wire name."""
        from pydantic import Field
        from taskq.ratelimit import KeyedRateLimitRef

        class MyPayload(BaseModel):
            tenant_id: str = Field(alias="tenantId")

        ref = KeyedRateLimitRef.typed(
            MyPayload,
            base_name="api-per-tenant",
            key_fn=lambda p: p.tenant_id,
            capacity=10.0,
            refill_per_second=1.0,
        )
        # Raw dict uses the wire alias "tenantId", model exposes "tenant_id"
        model = MyPayload.model_validate({"tenantId": "acme"})
        result = ref.key_fn(model)
        assert result == "acme"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_ratelimit_refs.py::TestKeyedRateLimitRefTyped -v`
Expected: FAIL — `KeyedRateLimitRef.typed` does not exist, `payload_type` not a field

- [ ] **Step 3: Make `payload_type` required, add `.typed()` to `KeyedRateLimitRef`**

In `src/taskq/ratelimit/refs.py`:

1. Change `key_fn` field type from `Callable[[dict[str, object]], str]` to `Callable[[BaseModel], str]`
2. Add `payload_type: type[BaseModel]` as a **required** field (no default, no `| None`)
3. Add the `.typed()` classmethod (see API Surface above)
4. Update the class docstring: replace dict-access examples with model-attribute examples
5. Update the module docstring: replace `key_fn=lambda p: p["tenant_id"]` with `key_fn=lambda p: p.tenant_id` and show `.typed()` construction

- [ ] **Step 4: Migrate all existing `KeyedRateLimitRef` constructions in tests**

This is the breaking-change migration. Every `KeyedRateLimitRef(...)` construction that doesn't pass `payload_type` must be updated. The pattern is:

**Helper migration** (`tests/test_ratelimit_keyed_rate_limits.py`):

```python
# BEFORE:
def _default_key_fn(payload: dict[str, object]) -> str:
    return str(payload["tenant_id"])

def _rate_limit_ref(
    base_name: str = "api-per-tenant",
    capacity: float = 10.0,
    refill_per_second: float = 1.0,
    key_fn: Callable[[dict[str, object]], str] = _default_key_fn,
) -> KeyedRateLimitRef:
    return KeyedRateLimitRef(
        base_name=base_name,
        key_fn=key_fn,
        capacity=capacity,
        refill_per_second=refill_per_second,
    )

# AFTER:
class _DefaultPayload(BaseModel):
    tenant_id: str

def _default_key_fn(payload: _DefaultPayload) -> str:
    return payload.tenant_id

def _rate_limit_ref(
    base_name: str = "api-per-tenant",
    capacity: float = 10.0,
    refill_per_second: float = 1.0,
    key_fn: Callable[[_DefaultPayload], str] = _default_key_fn,
) -> KeyedRateLimitRef:
    return KeyedRateLimitRef.typed(
        _DefaultPayload,
        base_name=base_name,
        key_fn=key_fn,
        capacity=capacity,
        refill_per_second=refill_per_second,
    )
```

**Custom `key_fn` lambdas:** Tests that pass `key_fn` lambdas accessing the payload as a dict (`p["..."]`) must change to model-attribute access (`p.attr`). Tests that pass `key_fn` lambdas that don't access the payload (e.g., `lambda _p: "fixed"`, `lambda _p: long_key`) need no lambda change, but the ref helper must pass `payload_type`.

**Direct constructions** (not via helper): Any test that constructs `KeyedRateLimitRef(...)` directly (e.g., `test_ratelimit_keyed_rate_limits_pg.py`, `test_di_validate.py`, `test_worker_di_bootstrap.py`, `test_queue_concurrency_cap.py`, `web_admin/test_ops.py`) must add `payload_type=SomeModel` and change `key_fn` from dict access to model-attribute access. Use `.typed()` for type safety.

**e2e actor** (`tests/e2e/actors.py`):

```python
# BEFORE:
KeyedRateLimitRef(
    base_name="e2e_per_tenant",
    key_fn=lambda p: p["tenant_id"],
    capacity=3,
    refill_per_second=1.0,
    backend="redis",
)

# AFTER:
KeyedRateLimitRef.typed(
    DeliverTenantWebhookPayload,
    base_name="e2e_per_tenant",
    key_fn=lambda p: p.tenant_id,
    capacity=3,
    refill_per_second=1.0,
    backend="redis",
)
```

**Test payloads:** Existing tests that pass `payload={"tenant_id": "acme"}` (a dict) to `_resolve_rate_limit_name` or `acquire_for_actor` still work — the registry validates the dict against `ref.payload_type` and passes the model to `key_fn`. No change needed to the payload dicts themselves, as long as the dict keys match the model's field names (or aliases).

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/test_ratelimit_refs.py::TestKeyedRateLimitRefTyped tests/test_ratelimit_keyed_rate_limits.py -v --timeout=30`
Expected: PASS — new tests pass, migrated existing tests pass

- [ ] **Step 6: Commit**

```bash
git add src/taskq/ratelimit/refs.py tests/
git commit -m "feat(ratelimit)!: make payload_type required, add .typed() to KeyedRateLimitRef

Breaking change: key_fn now receives the validated Pydantic model, not
the raw dict. Existing key_fn implementations that access p[\"key\"] must
be updated to p.attr. All existing test constructions migrated to .typed()."
```

---

### Task 2: Make `payload_type` required, add `.typed()` to `KeyedReservationRef`; migrate existing constructions

**Files:**
- Modify: `src/taskq/ratelimit/refs.py`
- Modify: `tests/test_ratelimit_keyed_refs.py` (migrate `_keyed_ref` helper + all constructions)
- Modify: `tests/test_keyed_reservation_hardening.py` (migrate `_keyed_ref` helper)
- Modify: `tests/test_ratelimit_keyed_refs_pg.py`
- Modify: `tests/test_ratelimit_composition.py`
- Modify: `tests/test_di_validate.py`
- Modify: `tests/test_actor.py`
- Modify: `tests/test_queue_concurrency_cap.py`
- Test: `tests/test_ratelimit_refs.py`

- [ ] **Step 1: Write failing tests for `KeyedReservationRef.typed()` and required `payload_type`**

Add to `tests/test_ratelimit_refs.py`:

> **Import note:** Ensure `BaseModel` and `Field` are imported from pydantic (see Task 1 Step 1 import note).

```python
class TestKeyedReservationRefTyped:
    def test_typed_classmethod_creates_ref_with_payload_type(self) -> None:
        """KeyedReservationRef.typed() stores payload_type and key_fn."""
        from datetime import timedelta
        from taskq.ratelimit import KeyedReservationRef

        class MyPayload(BaseModel):
            session_id: str

        ref = KeyedReservationRef.typed(
            MyPayload,
            base_name="session-cap",
            key_fn=lambda p: p.session_id,
            slots=3,
            lease=timedelta(minutes=5),
        )
        assert ref.payload_type is MyPayload
        assert ref.base_name == "session-cap"
        assert ref.slots == 3

    def test_payload_type_is_required(self) -> None:
        """Construction without payload_type raises ValidationError."""
        from datetime import timedelta
        from pydantic import ValidationError
        from taskq.ratelimit import KeyedReservationRef

        with pytest.raises(ValidationError):
            KeyedReservationRef(  # pyright: ignore[reportCallIssue]
                base_name="session-cap",
                key_fn=lambda p: str(p),
                slots=3,
                lease=timedelta(minutes=5),
                # payload_type missing — required
            )

    def test_typed_key_fn_with_alias(self) -> None:
        """key_fn from .typed() uses model attributes, not wire aliases."""
        from datetime import timedelta
        from pydantic import Field
        from taskq.ratelimit import KeyedReservationRef

        class MyPayload(BaseModel):
            session_id: str = Field(alias="sessionId")

        ref = KeyedReservationRef.typed(
            MyPayload,
            base_name="session-cap",
            key_fn=lambda p: p.session_id,
            slots=3,
            lease=timedelta(minutes=5),
        )
        model = MyPayload.model_validate({"sessionId": "abc123"})
        result = ref.key_fn(model)
        assert result == "abc123"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_ratelimit_refs.py::TestKeyedReservationRefTyped -v`
Expected: FAIL — `KeyedReservationRef.typed` does not exist

- [ ] **Step 3: Make `payload_type` required, add `.typed()` to `KeyedReservationRef`**

In `src/taskq/ratelimit/refs.py`:

1. Change `key_fn` field type from `Callable[[dict[str, object]], str]` to `Callable[[BaseModel], str]`
2. Add `payload_type: type[BaseModel]` as a **required** field
3. Add the `.typed()` classmethod (see API Surface above)
4. Update the class docstring: replace dict-access examples with model-attribute examples

- [ ] **Step 4: Migrate all existing `KeyedReservationRef` constructions in tests**

Same pattern as Task 1 Step 4. Migrate the helpers in:

- `tests/test_ratelimit_keyed_refs.py` — `_keyed_ref` helper + `_default_key_fn`
- `tests/test_keyed_reservation_hardening.py` — `_keyed_ref` helper + `_default_key_fn`
- `tests/test_ratelimit_keyed_refs_pg.py` — direct constructions
- `tests/test_ratelimit_composition.py` — direct constructions
- `tests/test_di_validate.py` — direct constructions
- `tests/test_actor.py` — direct construction
- `tests/test_queue_concurrency_cap.py` — direct constructions

**Helper migration pattern** (same for both test files):

```python
# BEFORE:
def _default_key_fn(payload: dict[str, object]) -> str:
    return str(payload["session_id"])

def _keyed_ref(
    base_name: str = "session-cap",
    slots: int = 3,
    lease: timedelta = timedelta(minutes=5),
    key_fn: Callable[[dict[str, object]], str] = _default_key_fn,
) -> KeyedReservationRef:
    return KeyedReservationRef(base_name=base_name, key_fn=key_fn, slots=slots, lease=lease)

# AFTER:
class _DefaultSessionPayload(BaseModel):
    session_id: str

def _default_key_fn(payload: _DefaultSessionPayload) -> str:
    return payload.session_id

def _keyed_ref(
    base_name: str = "session-cap",
    slots: int = 3,
    lease: timedelta = timedelta(minutes=5),
    key_fn: Callable[[_DefaultSessionPayload], str] = _default_key_fn,
) -> KeyedReservationRef:
    return KeyedReservationRef.typed(
        _DefaultSessionPayload,
        base_name=base_name,
        key_fn=key_fn,
        slots=slots,
        lease=lease,
    )
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/test_ratelimit_refs.py::TestKeyedReservationRefTyped tests/test_ratelimit_keyed_refs.py tests/test_keyed_reservation_hardening.py -v --timeout=30`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/taskq/ratelimit/refs.py tests/
git commit -m "feat(ratelimit)!: make payload_type required, add .typed() to KeyedReservationRef

Breaking change: key_fn now receives the validated Pydantic model, not
the raw dict. All existing test constructions migrated to .typed()."
```

---

### Task 3: Update `_resolve_rate_limit_name` to always validate/convert payload to model

**Files:**
- Modify: `src/taskq/ratelimit/registry.py`
- Test: `tests/test_ratelimit_keyed_rate_limits.py`

- [ ] **Step 1: Write failing tests for typed `_resolve_rate_limit_name`**

Add to `tests/test_ratelimit_keyed_rate_limits.py`:

```python
from pydantic import BaseModel, Field


class _TypedPayload(BaseModel):
    tenant_id: str
    region: str = "us-east-1"


class _AliasedPayload(BaseModel):
    tenant_id: str = Field(alias="tenantId")


# ── _resolve_rate_limit_name: key_fn always receives validated model ──


async def test_resolve_typed_ref_passes_validated_model_to_key_fn() -> None:
    """A KeyedRateLimitRef validates the dict and passes the model to
    key_fn — key_fn can access model attributes."""
    reg = RateLimitRegistry()
    ref = KeyedRateLimitRef.typed(
        _TypedPayload,
        base_name="api-per-tenant",
        key_fn=lambda p: p.tenant_id,
        capacity=10.0,
        refill_per_second=1.0,
    )

    name = await reg._resolve_rate_limit_name(  # pyright: ignore[reportPrivateUsage]
        ref, payload={"tenant_id": "acme"}, settings=None
    )

    assert name == "api-per-tenant:acme"


async def test_resolve_typed_ref_applies_pydantic_defaults() -> None:
    """A field with a Pydantic default is populated on the model even if
    absent from the raw dict — key_fn can read the default."""
    reg = RateLimitRegistry()
    ref = KeyedRateLimitRef.typed(
        _TypedPayload,
        base_name="api-per-tenant",
        key_fn=lambda p: f"{p.tenant_id}:{p.region}",
        capacity=10.0,
        refill_per_second=1.0,
    )

    # Raw dict has no "region" key — default "us-east-1" applies
    name = await reg._resolve_rate_limit_name(  # pyright: ignore[reportPrivateUsage]
        ref, payload={"tenant_id": "acme"}, settings=None
    )

    assert name == "api-per-tenant:acme:us-east-1"


async def test_resolve_typed_ref_applies_aliases() -> None:
    """A field with Field(alias=...) maps the wire name to the model
    attribute — key_fn uses the attribute, not the wire name."""
    reg = RateLimitRegistry()
    ref = KeyedRateLimitRef.typed(
        _AliasedPayload,
        base_name="api-per-tenant",
        key_fn=lambda p: p.tenant_id,
        capacity=10.0,
        refill_per_second=1.0,
    )

    # Raw dict uses the wire alias "tenantId"
    name = await reg._resolve_rate_limit_name(  # pyright: ignore[reportPrivateUsage]
        ref, payload={"tenantId": "acme"}, settings=None
    )

    assert name == "api-per-tenant:acme"


async def test_resolve_typed_ref_accepts_basemodel_payload_directly() -> None:
    """When the payload is already a validated BaseModel (the dispatch path
    passes validated_payload), the registry passes it directly to key_fn
    without re-validating."""
    reg = RateLimitRegistry()
    ref = KeyedRateLimitRef.typed(
        _TypedPayload,
        base_name="api-per-tenant",
        key_fn=lambda p: p.tenant_id,
        capacity=10.0,
        refill_per_second=1.0,
    )

    model = _TypedPayload(tenant_id="acme", region="eu-west-1")
    name = await reg._resolve_rate_limit_name(  # pyright: ignore[reportPrivateUsage]
        ref, payload=model, settings=None
    )

    assert name == "api-per-tenant:acme"


async def test_resolve_typed_ref_validation_error_propagates() -> None:
    """If the raw dict fails validation against payload_type, a
    ValidationError propagates (not a KeyError or limiter fault)."""
    from pydantic import ValidationError

    class StrictPayload(BaseModel):
        tenant_id: str

    reg = RateLimitRegistry()
    ref = KeyedRateLimitRef.typed(
        StrictPayload,
        base_name="api-per-tenant",
        key_fn=lambda p: p.tenant_id,
        capacity=10.0,
        refill_per_second=1.0,
    )

    with pytest.raises(ValidationError):
        await reg._resolve_rate_limit_name(  # pyright: ignore[reportPrivateUsage]
            ref, payload={"wrong_field": "x"}, settings=None
        )


async def test_resolve_typed_ref_wrong_model_type_re_validates() -> None:
    """When the payload is a BaseModel of a DIFFERENT type than the ref's
    payload_type, the registry re-validates against the ref's declared model
    instead of passing the wrong model to key_fn unchecked.

    This prevents AttributeError inside rate-limit resolution when a ref
    declares a narrowed/related model different from the actor's payload_type.
    """

    class ActorPayload(BaseModel):
        tenant_id: str
        region: str = "us-east-1"

    class RefPayload(BaseModel):
        tenant_id: str

    reg = RateLimitRegistry()
    ref = KeyedRateLimitRef.typed(
        RefPayload,
        base_name="api-per-tenant",
        key_fn=lambda p: p.tenant_id,
        capacity=10.0,
        refill_per_second=1.0,
    )

    # ActorPayload is NOT RefPayload — registry re-validates
    model = ActorPayload(tenant_id="acme", region="eu-west-1")
    name = await reg._resolve_rate_limit_name(  # pyright: ignore[reportPrivateUsage]
        ref, payload=model, settings=None
    )

    assert name == "api-per-tenant:acme"


async def test_resolve_typed_ref_same_model_type_zero_cost_passthrough() -> None:
    """When the payload is a BaseModel of the SAME type as the ref's
    payload_type, the registry passes it directly to key_fn without
    re-validation (zero-cost pass-through)."""
    received: list[object] = []
    reg = RateLimitRegistry()
    ref = KeyedRateLimitRef.typed(
        _TypedPayload,
        base_name="api-per-tenant",
        key_fn=lambda p: (received.append(p), p.tenant_id)[1],
        capacity=10.0,
        refill_per_second=1.0,
    )

    model = _TypedPayload(tenant_id="acme", region="eu-west-1")
    name = await reg._resolve_rate_limit_name(  # pyright: ignore[reportPrivateUsage]
        ref, payload=model, settings=None
    )

    assert name == "api-per-tenant:acme"
    # The exact same object was passed through (no re-validation/copy)
    assert received[0] is model  # type: ignore[union-attr]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_ratelimit_keyed_rate_limits.py -k "typed_ref or wrong_model or same_model" -v`
Expected: FAIL — `_resolve_rate_limit_name` does not accept `BaseModel` payloads, no isinstance check

- [ ] **Step 3: Update `_resolve_rate_limit_name` signature and logic**

In `src/taskq/ratelimit/registry.py`, update `_resolve_rate_limit_name`:

1. Change the `payload` parameter type from `dict[str, object] | None` to `dict[str, object] | BaseModel | None`
2. After the `isinstance(ref, str)` check and the `payload is None` check, replace the existing `key = self._validate_keyed_key(ref.key_fn(payload), ...)` call with the payload resolution logic (see API Surface above)
3. Add `from pydantic import BaseModel` to the imports at the top of the file (in the TYPE_CHECKING block is NOT sufficient since `isinstance(payload, BaseModel)` is a runtime check)

The logic is always the same — there is no untyped path:
```python
        # Always convert to validated model — key_fn always receives the model.
        if isinstance(payload, ref.payload_type):
            key_fn_arg: BaseModel = payload  # zero-cost pass-through
        elif isinstance(payload, BaseModel):
            key_fn_arg = ref.payload_type.model_validate(payload.model_dump())
        else:
            key_fn_arg = ref.payload_type.model_validate(payload)

        error_payload = (
            payload
            if isinstance(payload, dict)
            else payload.model_dump() if isinstance(payload, BaseModel) else None
        )

        key = self._validate_keyed_key(
            ref.key_fn(key_fn_arg),
            f"KeyedRateLimitRef(base_name={ref.base_name!r})",
            error_payload,
        )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_ratelimit_keyed_rate_limits.py -k "typed_ref or wrong_model or same_model" -v`
Expected: PASS

- [ ] **Step 5: Run existing rate limit tests to verify no regressions**

Run: `uv run pytest tests/test_ratelimit_keyed_rate_limits.py -v --timeout=30`
Expected: PASS — all existing tests pass (they use dict payloads, which the registry validates against `ref.payload_type`; `key_fn` accesses model attributes via the migrated helpers)

- [ ] **Step 6: Commit**

```bash
git add tests/test_ratelimit_keyed_rate_limits.py src/taskq/ratelimit/registry.py
git commit -m "feat(ratelimit): _resolve_rate_limit_name always validates payload to model"
```

---

### Task 4: Update `_resolve_reservation_name` to always validate/convert payload to model

**Files:**
- Modify: `src/taskq/ratelimit/registry.py`
- Test: `tests/test_ratelimit_keyed_refs.py`

- [ ] **Step 1: Write failing tests for typed `_resolve_reservation_name`**

Add to `tests/test_ratelimit_keyed_refs.py`:

```python
from pydantic import BaseModel, Field


class _TypedSessionPayload(BaseModel):
    session_id: str
    priority: str = "normal"


class _AliasedSessionPayload(BaseModel):
    session_id: str = Field(alias="sessionId")


# ── _resolve_reservation_name: key_fn always receives validated model ──


async def test_resolve_typed_reservation_ref_passes_validated_model_to_key_fn() -> None:
    """A KeyedReservationRef validates the dict and passes the model to
    key_fn."""
    from taskq.ratelimit.refs import KeyedReservationRef

    reg = RateLimitRegistry()
    ref = KeyedReservationRef.typed(
        _TypedSessionPayload,
        base_name="session-cap",
        key_fn=lambda p: p.session_id,
        slots=3,
        lease=timedelta(minutes=5),
    )

    name = await reg._resolve_reservation_name(  # pyright: ignore[reportPrivateUsage]
        ref, payload={"session_id": "abc123"}, pg_pool=None, settings=None
    )

    assert name == "session-cap:abc123"


async def test_resolve_typed_reservation_ref_applies_pydantic_defaults() -> None:
    """A field with a Pydantic default is populated on the model even if
    absent from the raw dict."""
    from taskq.ratelimit.refs import KeyedReservationRef

    reg = RateLimitRegistry()
    ref = KeyedReservationRef.typed(
        _TypedSessionPayload,
        base_name="session-cap",
        key_fn=lambda p: f"{p.session_id}:{p.priority}",
        slots=3,
        lease=timedelta(minutes=5),
    )

    name = await reg._resolve_reservation_name(  # pyright: ignore[reportPrivateUsage]
        ref, payload={"session_id": "s1"}, pg_pool=None, settings=None
    )

    assert name == "session-cap:s1:normal"


async def test_resolve_typed_reservation_ref_applies_aliases() -> None:
    """A field with Field(alias=...) maps the wire name to the model
    attribute."""
    from taskq.ratelimit.refs import KeyedReservationRef

    reg = RateLimitRegistry()
    ref = KeyedReservationRef.typed(
        _AliasedSessionPayload,
        base_name="session-cap",
        key_fn=lambda p: p.session_id,
        slots=3,
        lease=timedelta(minutes=5),
    )

    name = await reg._resolve_reservation_name(  # pyright: ignore[reportPrivateUsage]
        ref, payload={"sessionId": "abc123"}, pg_pool=None, settings=None
    )

    assert name == "session-cap:abc123"


async def test_resolve_typed_reservation_ref_accepts_basemodel_payload_directly() -> None:
    """When the payload is already a BaseModel, the registry passes it
    directly to key_fn without re-validating."""
    from taskq.ratelimit.refs import KeyedReservationRef

    reg = RateLimitRegistry()
    ref = KeyedReservationRef.typed(
        _TypedSessionPayload,
        base_name="session-cap",
        key_fn=lambda p: p.session_id,
        slots=3,
        lease=timedelta(minutes=5),
    )

    model = _TypedSessionPayload(session_id="abc123", priority="high")
    name = await reg._resolve_reservation_name(  # pyright: ignore[reportPrivateUsage]
        ref, payload=model, pg_pool=None, settings=None
    )

    assert name == "session-cap:abc123"


async def test_resolve_typed_reservation_ref_validation_error_propagates() -> None:
    """If the raw dict fails validation against payload_type, a
    ValidationError propagates (not a KeyError or limiter fault)."""
    from pydantic import ValidationError
    from taskq.ratelimit.refs import KeyedReservationRef

    class StrictPayload(BaseModel):
        session_id: str

    reg = RateLimitRegistry()
    ref = KeyedReservationRef.typed(
        StrictPayload,
        base_name="session-cap",
        key_fn=lambda p: p.session_id,
        slots=3,
        lease=timedelta(minutes=5),
    )

    with pytest.raises(ValidationError):
        await reg._resolve_reservation_name(  # pyright: ignore[reportPrivateUsage]
            ref, payload={"wrong_field": "x"}, pg_pool=None, settings=None
        )


async def test_resolve_typed_reservation_ref_wrong_model_type_re_validates() -> None:
    """When the payload is a BaseModel of a DIFFERENT type than the ref's
    payload_type, the registry re-validates against the ref's declared model
    instead of passing the wrong model to key_fn unchecked."""

    class ActorPayload(BaseModel):
        session_id: str
        extra: str = "x"

    class RefPayload(BaseModel):
        session_id: str

    from taskq.ratelimit.refs import KeyedReservationRef

    reg = RateLimitRegistry()
    ref = KeyedReservationRef.typed(
        RefPayload,
        base_name="session-cap",
        key_fn=lambda p: p.session_id,
        slots=3,
        lease=timedelta(minutes=5),
    )

    model = ActorPayload(session_id="abc123", extra="y")
    name = await reg._resolve_reservation_name(  # pyright: ignore[reportPrivateUsage]
        ref, payload=model, pg_pool=None, settings=None
    )

    assert name == "session-cap:abc123"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_ratelimit_keyed_refs.py -k "typed_reservation_ref or wrong_model" -v`
Expected: FAIL — `_resolve_reservation_name` does not accept `BaseModel` payloads

- [ ] **Step 3: Update `_resolve_reservation_name` signature and logic**

In `src/taskq/ratelimit/registry.py`, apply the same changes as Task 3 Step 3 to `_resolve_reservation_name`:

1. Change the `payload` parameter type from `dict[str, object] | None` to `dict[str, object] | BaseModel | None`
2. Replace the existing `key = self._validate_keyed_key(ref.key_fn(payload), ...)` call with the same payload resolution logic:

```python
        # Always convert to validated model — key_fn always receives the model.
        if isinstance(payload, ref.payload_type):
            key_fn_arg: BaseModel = payload  # zero-cost pass-through
        elif isinstance(payload, BaseModel):
            key_fn_arg = ref.payload_type.model_validate(payload.model_dump())
        else:
            key_fn_arg = ref.payload_type.model_validate(payload)

        error_payload = (
            payload
            if isinstance(payload, dict)
            else payload.model_dump() if isinstance(payload, BaseModel) else None
        )

        key = self._validate_keyed_key(
            ref.key_fn(key_fn_arg),
            f"KeyedReservationRef(base_name={ref.base_name!r})",
            error_payload,
            empty_key_msg="an empty key or non-string value",
        )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_ratelimit_keyed_refs.py -k "typed_reservation_ref or wrong_model" -v`
Expected: PASS

- [ ] **Step 5: Run existing reservation ref tests to verify no regressions**

Run: `uv run pytest tests/test_ratelimit_keyed_refs.py tests/test_keyed_reservation_hardening.py -v --timeout=30`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add tests/test_ratelimit_keyed_refs.py src/taskq/ratelimit/registry.py
git commit -m "feat(ratelimit): _resolve_reservation_name always validates payload to model"
```

---

### Task 5: Update `acquire_for_actor` signature to accept `BaseModel | dict | None`

**Files:**
- Modify: `src/taskq/ratelimit/registry.py`
- Test: `tests/test_ratelimit_keyed_rate_limits.py`

- [ ] **Step 1: Write failing tests for `acquire_for_actor` with `BaseModel` payload**

Add to `tests/test_ratelimit_keyed_rate_limits.py`:

```python
async def test_acquire_for_actor_accepts_basemodel_payload_with_typed_ref() -> None:
    """acquire_for_actor accepts a BaseModel payload — key_fn receives
    the model directly (zero-cost pass-through for same-type)."""
    clock = FakeClock(_START)
    reg = RateLimitRegistry()
    reg.register(
        TokenBucket(name="api-per-tenant:acme", capacity=1, refill_per_second=0, backend="memory")
    )
    ref = KeyedRateLimitRef.typed(
        _TypedPayload,
        base_name="api-per-tenant",
        key_fn=lambda p: p.tenant_id,
        capacity=1,
        refill_per_second=0,
        backend="memory",
    )

    model = _TypedPayload(tenant_id="acme")
    acquired = await reg.acquire_for_actor(
        rate_limits=[ref],
        reservations=[],
        job_id=new_uuid(),
        worker_id=new_uuid(),
        payload=model,  # ← BaseModel, not dict
        clock=clock,
    )
    assert acquired[0].name == "api-per-tenant:acme"


async def test_acquire_for_actor_typed_ref_with_dict_payload_validates() -> None:
    """acquire_for_actor with a dict payload and a typed ref validates
    the dict against payload_type before calling key_fn."""
    clock = FakeClock(_START)
    reg = RateLimitRegistry()
    reg.register(
        TokenBucket(name="api-per-tenant:acme", capacity=1, refill_per_second=0, backend="memory")
    )
    ref = KeyedRateLimitRef.typed(
        _AliasedPayload,
        base_name="api-per-tenant",
        key_fn=lambda p: p.tenant_id,
        capacity=1,
        refill_per_second=0,
        backend="memory",  # ← required: default "redis" would raise RuntimeError (H2)
    )

    # Dict uses wire alias "tenantId"; model_validate maps it to tenant_id
    acquired = await reg.acquire_for_actor(
        rate_limits=[ref],
        reservations=[],
        job_id=new_uuid(),
        worker_id=new_uuid(),
        payload={"tenantId": "acme"},  # ← raw dict with wire alias
        clock=clock,
    )
    assert acquired[0].name == "api-per-tenant:acme"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_ratelimit_keyed_rate_limits.py -k "acquire_for_actor_accepts_basemodel or acquire_for_actor_typed_ref_with_dict" -v`
Expected: FAIL — `acquire_for_actor` does not accept `BaseModel` payloads

- [ ] **Step 3: Update `acquire_for_actor` signature**

In `src/taskq/ratelimit/registry.py`, update the `acquire_for_actor` method:

1. Change the `payload` parameter type from `dict[str, object] | None` to `dict[str, object] | BaseModel | None`
2. Update the docstring to mention the `BaseModel` case
3. The internal calls to `_resolve_reservation_name` and `_resolve_rate_limit_name` already receive the `payload` parameter — since those methods were updated in Tasks 3 and 4 to accept `BaseModel | dict | None`, no further changes are needed in `acquire_for_actor` itself beyond the type annotation

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_ratelimit_keyed_rate_limits.py -k "acquire_for_actor_accepts_basemodel or acquire_for_actor_typed_ref_with_dict" -v`
Expected: PASS

- [ ] **Step 5: Run all existing rate limit tests to verify no regressions**

Run: `uv run pytest tests/test_ratelimit_keyed_rate_limits.py tests/test_ratelimit_keyed_refs.py tests/test_ratelimit_composition.py tests/test_ratelimit_composition_integration.py -v --timeout=30`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add tests/test_ratelimit_keyed_rate_limits.py src/taskq/ratelimit/registry.py
git commit -m "feat(ratelimit): acquire_for_actor accepts BaseModel payload"
```

---

### Task 6: Fix `_consumer.py` to unconditionally pass `validated_payload` to `acquire_for_actor`

**Files:**
- Modify: `src/taskq/worker/_consumer.py`
- Test: `tests/test_consumer.py`

- [ ] **Step 1: Write failing tests for consumer passing validated model**

Add to `tests/test_consumer.py`:

> **Import note (H3):** These tests use the existing `_FakeBackend` / `as_backend` harness (already imported at the top of `tests/test_consumer.py`). Do NOT pass `backend=None` — the success path and `except Exception` path both perform terminal writes via `backend`, which would crash with `AttributeError: 'NoneType'` before assertions execute.

```python
async def test_consumer_passes_validated_model_to_key_fn() -> None:
    """consume_one_job always passes the validated BaseModel to
    acquire_for_actor (not the raw dict), so a typed KeyedRateLimitRef
    with aliases receives the model with aliases applied."""
    from datetime import UTC, datetime
    from pydantic import BaseModel, Field
    from taskq.ratelimit import KeyedRateLimitRef, RateLimitRegistry
    from taskq.testing.clock import FakeClock
    from taskq.testing.jobs import make_job_row
    from taskq.worker._consumer import consume_one_job

    class ApiPayload(BaseModel):
        tenant_id: str = Field(alias="tenantId")

    key_fn_received: list[object] = []
    ref = KeyedRateLimitRef.typed(
        ApiPayload,
        base_name="test-api",
        key_fn=lambda p: (key_fn_received.append(p), p.tenant_id)[1],
        capacity=1,
        refill_per_second=0,
        backend="memory",
    )

    reg = RateLimitRegistry()
    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    backend = _FakeBackend()

    job = make_job_row(
        actor="test_actor",
        payload={"tenantId": "acme"},  # wire alias, not model attribute name
    )

    async def _run_actor(job_row, ctx):
        return None

    from taskq.retry import RetryPolicy
    from taskq.testing.actor import StubActorConfig

    await consume_one_job(
        backend=as_backend(backend),
        job=job,
        worker_id=new_uuid(),
        run_actor=_run_actor,
        actor_config=StubActorConfig(
            retry=RetryPolicy(),
            non_retryable_exceptions=(),
        ),
        payload_type=ApiPayload,
        clock=clock,
        rate_limit_registry=reg,
        rate_limits=[ref],
        reservations=[],
        validated_payload=ApiPayload.model_validate({"tenantId": "acme"}),
    )

    # key_fn received the validated model (with alias applied), not the raw dict
    assert len(key_fn_received) == 1
    assert isinstance(key_fn_received[0], ApiPayload)
    assert key_fn_received[0].tenant_id == "acme"  # type: ignore[union-attr]


async def test_consumer_validates_payload_before_acquire_for_direct_callers() -> None:
    """When validated_payload is None (direct caller, not dispatch path),
    consume_one_job validates job.payload before acquire_for_actor — so a
    ValidationError surfaces before a rate-limit token is consumed."""
    from datetime import UTC, datetime
    from pydantic import BaseModel, ValidationError
    from taskq.ratelimit import KeyedRateLimitRef, RateLimitRegistry
    from taskq.testing.clock import FakeClock
    from taskq.testing.jobs import make_job_row
    from taskq.worker._consumer import consume_one_job

    class StrictPayload(BaseModel):
        tenant_id: str  # required, no default

    ref = KeyedRateLimitRef.typed(
        StrictPayload,
        base_name="test-api",
        key_fn=lambda p: p.tenant_id,
        capacity=1,
        refill_per_second=0,
        backend="memory",
    )

    reg = RateLimitRegistry()
    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    backend = _FakeBackend()

    job = make_job_row(
        actor="test_actor",
        payload={"wrong_field": "x"},  # missing tenant_id — will fail validation
    )

    async def _run_actor(job_row, ctx):
        return None

    from taskq.retry import RetryPolicy
    from taskq.testing.actor import StubActorConfig

    with pytest.raises(ValidationError):
        await consume_one_job(
            backend=as_backend(backend),
            job=job,
            worker_id=new_uuid(),
            run_actor=_run_actor,
            actor_config=StubActorConfig(
                retry=RetryPolicy(),
                non_retryable_exceptions=(),
            ),
            payload_type=StrictPayload,
            clock=clock,
            rate_limit_registry=reg,
            rate_limits=[ref],
            reservations=[],
            validated_payload=None,  # direct caller — no pre-validated model
        )
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_consumer.py::test_consumer_passes_validated_model_to_key_fn tests/test_consumer.py::test_consumer_validates_payload_before_acquire_for_direct_callers -v`
Expected: FAIL — `_consumer.py` passes `job.payload` (raw dict); the model test fails because `key_fn` receives a dict (not the model); the validation test fails because validation happens after acquire, not before

- [ ] **Step 3: Fix `_consumer.py` — move validation before acquire, always pass validated_payload**

In `src/taskq/worker/_consumer.py`:

1. **Move the `validated_payload` fallback before `acquire_for_actor`:**

Before the `if _needs_acquire and rate_limit_registry is not None:` block (line ~303), add:

```python
    # Ensure the validated model is available before rate-limit acquisition.
    # On the dispatch path, validated_payload is already set (dispatch_one_job
    # validates at line 180). For direct callers, validate here. A
    # ValidationError from an invalid payload now surfaces BEFORE a
    # rate-limit token is consumed — the correct behavior: don't acquire
    # a token for a job with an invalid payload.
    if validated_payload is None:
        validated_payload = payload_type.model_validate(job.payload)
```

2. **Change `payload=job.payload` to `payload=validated_payload`:**

```python
            acquired = await rate_limit_registry.acquire_for_actor(
                rate_limits=_rl_limits,
                reservations=_rl_reservations,
                job_id=job.id,
                worker_id=worker_id,
                payload=validated_payload,  # ← ALWAYS the model, no gating
                redis_client=redis_client,
                pg_pool=worker_pool,
                clock=clock,
                settings=settings,
            )
```

3. **Remove the later `validated_payload` fallback** (lines ~357-361) since `validated_payload` is now always set before that point:

```python
    # Remove this block — validated_payload is already set above:
    # validated_payload = (
    #     validated_payload
    #     if validated_payload is not None
    #     else payload_type.model_validate(job.payload)
    # )
```

No gating. No `any(r.payload_type is not None)` check. No conditional. `validated_payload` is always a `BaseModel` when it reaches `acquire_for_actor`. This is a **breaking change** for any `key_fn` that accesses raw dict keys — which is the point.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_consumer.py::test_consumer_passes_validated_model_to_key_fn tests/test_consumer.py::test_consumer_validates_payload_before_acquire_for_direct_callers -v`
Expected: PASS

- [ ] **Step 5: Run existing consumer tests to verify no regressions**

Run: `uv run pytest tests/test_consumer.py tests/test_dispatch_one_job.py tests/test_dispatch.py -v --timeout=60`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add tests/test_consumer.py src/taskq/worker/_consumer.py
git commit -m "fix(consumer)!: always pass validated_payload to acquire_for_actor

Breaking change: _consumer.py now unconditionally passes the validated
BaseModel (not the raw dict) to acquire_for_actor. The validated_payload
fallback is moved before acquire so a ValidationError surfaces before a
rate-limit token is consumed. key_fn implementations that accessed raw
dict keys must be updated to model attributes."
```

---

### Task 7: Add e2e actor and test for typed `key_fn` with aliases

**Files:**
- Modify: `tests/e2e/actors.py`
- Modify: `tests/e2e/worker_entry.py`
- Test: `tests/e2e/test_keyed_rate_limit.py`

- [ ] **Step 1: Add a typed-keyed-rate-limit actor to `tests/e2e/actors.py`**

Add near the existing `deliver_tenant_webhook` actor (which was already migrated to `.typed()` in Task 1):

```python
class TypedTenantPayload(BaseModel):
    model_config = ConfigDict(validate_by_name=True, serialize_by_alias=True)

    run_id: str
    tenant_id: str = Field(alias="tenantId")
    endpoint_id: str


@actor(
    name="deliver_typed_tenant_webhook",
    queue="e2e",
    rate_limits=[
        KeyedRateLimitRef.typed(
            TypedTenantPayload,
            base_name="e2e_typed_per_tenant",
            key_fn=lambda p: p.tenant_id,
            capacity=3,
            refill_per_second=1.0,
            backend="redis",
        )
    ],
)
async def deliver_typed_tenant_webhook(
    payload: TypedTenantPayload,
    ctx: JobContext[TypedTenantPayload],
    *,
    pool: asyncpg.Pool,
) -> None:
    """Typed-keyed-rate-limit e2e actor.

    Uses KeyedRateLimitRef.typed with a payload model that has
    Field(alias="tenantId"). The key_fn accesses p.tenant_id (the model
    attribute), proving the validated model is passed — not the raw dict
    with "tenantId" as the wire name.
    """
    await asyncio.sleep(0.03)
    await _record_effect(
        pool,
        ctx,
        "typed_tenant_delivered",
        {
            "run_id": payload.run_id,
            "tenant_id": payload.tenant_id,
            "endpoint_id": payload.endpoint_id,
        },
    )
```

> **Import note:** Add `ConfigDict` to the pydantic import in `tests/e2e/actors.py`: `from pydantic import BaseModel, ConfigDict, Field`.

Note: `TypedTenantPayload` uses `model_config = ConfigDict(validate_by_name=True, serialize_by_alias=True)` (H4) so that:

1. **Construction by field name works** (`validate_by_name=True`): `TypedTenantPayload(run_id=..., tenant_id=tenant_a, ...)` succeeds — without this, an aliased field requires the alias (`tenantId`) at init.
2. **The stored row carries the wire alias** (`serialize_by_alias=True`): the client enqueue path (`client/_args.py:118`) serializes via `model_dump(mode="json")`, which with `serialize_by_alias=True` emits `tenantId` (not `tenant_id`). `dispatch_one_job:180` then re-validates against the aliased model, accepting the alias.
3. **A raw-dict `key_fn` would genuinely fail**: the dict key is `"tenantId"`, not `"tenant_id"` — so `p["tenant_id"]` raises `KeyError` and `p.tenant_id` on a dict raises `AttributeError`. This exercises the alias end-to-end.

- [ ] **Step 2: Register the actor in `tests/e2e/worker_entry.py`**

In `tests/e2e/worker_entry.py`, add to the imports and the actors dict:

```python
from .actors import (
    # ... existing imports ...
    deliver_typed_tenant_webhook,
    TypedTenantPayload,
)

ACTORS = {
    # ... existing entries ...
    "deliver_typed_tenant_webhook": deliver_typed_tenant_webhook,
}
```

- [ ] **Step 3: Write the e2e test**

Add to `tests/e2e/test_keyed_rate_limit.py`:

```python
async def test_typed_keyed_rate_limit_with_aliases(
    e2e_client: TaskQ,
    e2e_worker: E2EWorker,
    e2e_pg_pool: asyncpg.Pool,
    e2e_schema: E2ESchema,
    run_id: str,
) -> None:
    """A KeyedRateLimitRef.typed with an aliased payload model resolves
    the key from the model attribute, not the wire name.

    The actor ``deliver_typed_tenant_webhook`` uses
    ``KeyedRateLimitRef.typed(TypedTenantPayload, ...)`` where
    ``TypedTenantPayload`` has ``Field(alias="tenantId")``. The
    ``key_fn`` accesses ``p.tenant_id`` (model attribute), which would
    raise ``AttributeError`` if the raw dict (with key ``"tenantId"``)
    were passed instead of the validated model.

    This test verifies:
    1. The actor runs successfully (key_fn received the model, not the dict).
    2. Per-tenant independence holds: two tenants get separate buckets.
    """
    from .actors import TypedTenantPayload, deliver_typed_tenant_webhook

    tenant_a = "gamma"
    tenant_b = "delta"

    handles_a = [
        await e2e_client.enqueue(
            deliver_typed_tenant_webhook,
            TypedTenantPayload(
                run_id=run_id,
                tenant_id=tenant_a,
                endpoint_id=f"ep-{i}",
            ),
        )
        for i in range(3)
    ]

    handles_b = [
        await e2e_client.enqueue(
            deliver_typed_tenant_webhook,
            TypedTenantPayload(
                run_id=run_id,
                tenant_id=tenant_b,
                endpoint_id=f"ep-{i}",
            ),
        )
        for i in range(3)
    ]

    await asyncio.gather(
        *(h.wait(timeout=90) for h in [*handles_a, *handles_b])
    )

    # All 6 jobs should succeed — each tenant gets a fresh capacity-3 bucket.
    a_rows = await fetch_job_rows(
        e2e_pg_pool, e2e_schema.schema_name, [h.job_id for h in handles_a]
    )
    b_rows = await fetch_job_rows(
        e2e_pg_pool, e2e_schema.schema_name, [h.job_id for h in handles_b]
    )

    # Verify no denials. Use a local bucket base matching the actor's
    # base_name so the assertion is non-vacuous (M2: _denied_count hardcodes
    # _BUCKET_BASE = "e2e_per_tenant" which doesn't match this actor's
    # "e2e_typed_per_tenant").
    _TYPED_BUCKET_BASE = "e2e_typed_per_tenant"
    a_denied = sum(
        1
        for row in a_rows
        if (awaiting := json.loads(row["metadata"]).get("awaiting")) is not None
        and awaiting.endswith(f"{_TYPED_BUCKET_BASE}:{tenant_a}")
    )
    b_denied = sum(
        1
        for row in b_rows
        if (awaiting := json.loads(row["metadata"]).get("awaiting")) is not None
        and awaiting.endswith(f"{_TYPED_BUCKET_BASE}:{tenant_b}")
    )

    # Both tenants should have 0 denials (fresh capacity-3 buckets).
    assert a_denied == 0, f"tenant A had {a_denied} denials (expected 0)"
    assert b_denied == 0, f"tenant B had {b_denied} denials (expected 0)"

    # Verify effects were recorded.
    effects = await fetch_effects(
        e2e_pg_pool, e2e_schema.schema_name, run_id, kind="typed_tenant_delivered"
    )
    assert len(effects) == 6
```

- [ ] **Step 4: Run the e2e test**

Run: `uv run pytest tests/e2e/test_keyed_rate_limit.py::test_typed_keyed_rate_limit_with_aliases -v --timeout=900`
Expected: PASS — the actor runs successfully, proving `key_fn` received the validated model with aliases applied

- [ ] **Step 5: Run existing e2e keyed rate limit test to verify no regression**

Run: `uv run pytest tests/e2e/test_keyed_rate_limit.py::test_keyed_rate_limit_per_tenant_independence -v --timeout=900`
Expected: PASS — the migrated `deliver_tenant_webhook` actor (now using `.typed()`) still works

- [ ] **Step 6: Commit**

```bash
git add tests/e2e/actors.py tests/e2e/worker_entry.py tests/e2e/test_keyed_rate_limit.py
git commit -m "test(e2e): add typed keyed rate limit test with aliased payload model"
```

---

### Task 8: Update documentation

**Files:**
- Modify: `docs/guides/rate-limiting.md`
- Modify: `docs/architecture.md`

- [ ] **Step 1: Update `docs/guides/rate-limiting.md`**

In the `KeyedRateLimitRef` section, update the `key_fn` description:

Replace the text:
```
`key_fn` receives the actor's validated payload as a `dict[str, object]` (the same shape stored
on the job row) and must return a non-empty string.
```

With:
```
`key_fn` receives the actor's validated Pydantic model and must return a non-empty string.
The model has defaults, aliases, and validation applied. Use the `.typed()` classmethod for
compile-time type checking of `key_fn` against the payload model:

```python
from pydantic import BaseModel, Field
from taskq.ratelimit import KeyedRateLimitRef

class ApiRequest(BaseModel):
    tenant_id: str = Field(alias="tenantId")  # wire name differs from attribute

KeyedRateLimitRef.typed(
    ApiRequest,
    base_name="api-per-tenant",
    key_fn=lambda p: p.tenant_id,  # p is ApiRequest — typed, alias applied
    capacity=10,
    refill_per_second=1.0,
)
```

The `payload_type` field (required) declares the model class the registry validates the
payload against before calling `key_fn`. Pydantic defaults populate fields absent from the
serialized payload, aliases map wire names to model attributes, and validation errors surface
as `ValidationError` (a payload error) rather than `KeyError` (a limiter fault).
```

Apply the same update to the `KeyedReservationRef` section.

- [ ] **Step 2: Update `docs/architecture.md`**

In the "Dispatch integration" section, update to mention the validated model is always passed to `acquire_for_actor`:

Replace:
```
Before executing the actor body, `consume_one_job` checks the rate-limit decision:
```

With:
```
Before executing the actor body, `consume_one_job` checks the rate-limit decision. The
already-validated payload model (constructed in `dispatch_one_job` and passed as
`validated_payload`) is passed to `acquire_for_actor`. `KeyedRateLimitRef` and
`KeyedReservationRef` declare a required `payload_type`; the registry validates the payload
against it and `key_fn` always receives the validated Pydantic model:
```

- [ ] **Step 3: Commit**

```bash
git add docs/guides/rate-limiting.md docs/architecture.md
git commit -m "docs: update rate-limiting guide and architecture for typed key_fn"
```

---

### Task 9: Full test suite verification

- [ ] **Step 1: Run the full unit test suite**

Run: `uv run pytest tests/ -x --timeout=60 -q`
Expected: PASS — no regressions

- [ ] **Step 2: Run type checking**

Run: `uv run pyright src/taskq/ratelimit/refs.py src/taskq/ratelimit/registry.py src/taskq/worker/_consumer.py`
Expected: PASS — no new type errors (existing `type: ignore` comments are documented)

- [ ] **Step 3: Run linting**

Run: `uv run ruff check src/taskq/ratelimit/refs.py src/taskq/ratelimit/registry.py src/taskq/worker/_consumer.py tests/test_ratelimit_refs.py tests/test_ratelimit_keyed_rate_limits.py tests/test_ratelimit_keyed_refs.py tests/test_consumer.py`
Expected: PASS

- [ ] **Step 4: Run the e2e keyed rate limit tests**

Run: `uv run pytest tests/e2e/test_keyed_rate_limit.py -v --timeout=900`
Expected: PASS

- [ ] **Step 5: Commit any remaining changes**

```bash
git add -A
git commit -m "chore: full test suite verification for typed key_fn"
```

---

## Test Coverage Requirements

### Unit tests (must have)

1. **`.typed()` classmethod construction** — both refs
   - Stores `payload_type` correctly
   - `key_fn` receives the validated model, not the dict
   - `backend` parameter forwarded (for `KeyedRateLimitRef`)

2. **`payload_type` is required** — both refs
   - Construction without `payload_type` raises `ValidationError`

3. **Pydantic defaults applied** — both refs
   - A field with a default that is absent from the raw dict is populated on the model
   - `key_fn` can read the default value from the model

4. **Aliases applied** — both refs
   - `Field(alias="wireName")` maps the wire name to the model attribute
   - `key_fn` uses the model attribute, not the wire name

5. **`BaseModel` payload accepted directly** — both refs
   - When the payload is already a `BaseModel` of the same type as `ref.payload_type`, it is passed directly to `key_fn` (zero-cost pass-through, no re-validation)
   - When the payload is a `BaseModel` of a **different** type, the registry re-validates via `ref.payload_type.model_validate(payload.model_dump())` — preventing `AttributeError` from wrong-model payloads (H1)

6. **Raw dict validated against `payload_type`** — both refs
   - When the payload is a dict, `model_validate` is called against `ref.payload_type`
   - The resulting model is passed to `key_fn`

7. **`ValidationError` propagation** — both refs
   - An invalid payload dict raises `ValidationError`, not `KeyError`
   - The error surfaces before `key_fn` is called

8. **`acquire_for_actor` accepts `BaseModel`** — both refs
   - Typed ref + `BaseModel` payload (same type) → `key_fn` receives model (zero-cost)
   - Typed ref + `BaseModel` payload (wrong type) → re-validates against ref's model
   - Typed ref + dict payload → validates and passes model

9. **Consumer always passes `validated_payload`** — `_consumer.py`
   - `consume_one_job` always passes `validated_payload` (BaseModel) to `acquire_for_actor` — no gating, no conditional
   - Typed `key_fn` with aliases receives the model with aliases applied
   - When `validated_payload` is `None` (direct caller), `consume_one_job` validates `job.payload` before `acquire_for_actor` — a `ValidationError` surfaces before a rate-limit token is consumed

### E2E tests (must have)

1. **Typed keyed rate limit with aliases**
   - Actor uses `KeyedRateLimitRef.typed` with `Field(alias="tenantId")`
   - `key_fn` accesses `p.tenant_id` (model attribute, not wire name)
   - Actor runs successfully — proving the validated model is passed
   - Per-tenant independence holds

2. **Existing e2e actor still works after migration**
   - `deliver_tenant_webhook` migrated from `KeyedRateLimitRef(key_fn=lambda p: p["tenant_id"])` to `KeyedRateLimitRef.typed(DeliverTenantWebhookPayload, key_fn=lambda p: p.tenant_id)`
   - `test_keyed_rate_limit_per_tenant_independence` still passes

## Breaking Changes

This is a **breaking change**, intentional and correct for a pre-1.0 library heading to 1.0. The downstream consumers (warden, cennan, aacrtool) are waiting on these features and have no shipped code that needs preserving.

### What breaks

1. **`KeyedRateLimitRef` and `KeyedReservationRef` construction** — `payload_type` is now a **required** field. Any construction that omits it raises `ValidationError`. Use `.typed()` for type-safe construction:

   ```python
   # BEFORE (broken):
   KeyedRateLimitRef(base_name="api-per-tenant", key_fn=lambda p: p["tenant_id"], capacity=10, refill_per_second=1.0)

   # AFTER:
   KeyedRateLimitRef.typed(MyPayload, base_name="api-per-tenant", key_fn=lambda p: p.tenant_id, capacity=10, refill_per_second=1.0)
   ```

2. **`key_fn` signature changes** from `dict[str, object]` to the validated model type. `key_fn` implementations that access raw dict keys must be updated to access model attributes:

   ```python
   # BEFORE (broken):
   key_fn=lambda p: p["tenantId"]      # dict access, wire name
   key_fn=lambda p: str(p["tenant_id"]) # dict access, field name

   # AFTER:
   key_fn=lambda p: p.tenant_id         # model attribute access
   key_fn=lambda p: p.tenant_id          # model attribute, alias applied
   ```

3. **`_consumer.py` always passes `validated_payload` (BaseModel)** to `acquire_for_actor` — unconditionally, no gating. Any `key_fn` that expects a raw dict will receive a `BaseModel` instead and raise `AttributeError` (e.g., `p["tenant_id"]` on a `BaseModel` raises `TypeError`). This is the point — the strictly better, typesafe design the issue asked for.

4. **`_consumer.py` validates `job.payload` before `acquire_for_actor`** when `validated_payload` is `None` (direct callers). A `ValidationError` from an invalid payload now surfaces before a rate-limit token is consumed. This is the correct behavior: don't acquire a token for a job with an invalid payload.

### What does NOT change

1. **`@actor` decorator** — No signature change. The `rate_limits` and `reservations` parameters already accept `list[str | KeyedRateLimitRef]` and `list[str | KeyedReservationRef]`. Refs with `payload_type` are still `KeyedRateLimitRef` / `KeyedReservationRef` instances.

2. **DI validation at startup** — Unchanged. The DI validation phase checks static names against the registry; it does not inspect `payload_type` or `key_fn` signatures.

3. **Settings** — No new settings. `max_keyed_rate_limits` and `max_keyed_reservations` are unaffected.

4. **`acquire_for_actor` callers passing dicts** — The `payload` parameter accepts `dict[str, object] | BaseModel | None`. Direct callers (tests, non-dispatch callers) can still pass dicts; the registry validates them against `ref.payload_type` and passes the model to `key_fn`. The production dispatch path always passes a `BaseModel`.

### Risk assessment

| Risk | Severity | Mitigation |
|------|----------|------------|
| `key_fn` implementations accessing raw dict keys break with `AttributeError`/`TypeError` | **Expected (breaking change)** | This is the intended behavior. All `key_fn` implementations must be updated to access model attributes. The `.typed()` classmethod provides compile-time type checking so pyright catches missed migrations. |
| `ValidationError` from typed ref validation surfaces in wrong layer | Low | On the **dispatch path**: the payload arrives as a `BaseModel`, so the registry's typed path uses `isinstance` + pass-through or re-validation. A `ValidationError` from re-validation (wrong model type) propagates from `_resolve_*_name` → `acquire_for_actor`. The `acquire_for_actor` call (`_consumer.py:305-315`) is in a `try` that catches only `ReservationUnavailable`; a `ValidationError` propagates out of `consume_one_job` to `dispatch_one_job`'s `except Exception` (`dispatch.py:300`) → `_handle_generic_exception`, which routes it as a job failure (M1). On the **direct-call path** (tests, `taskq.testing._runner`): a `ValidationError` from `model_validate(dict)` propagates directly to the caller. In both cases, a malformed payload fails the job, not the limiter — the correct behavior. |
| Double validation (dispatch validates, then registry validates again) | None | When the payload is already a `BaseModel` of the same type as `ref.payload_type` (the common dispatch case), the registry passes it directly to `key_fn` via `isinstance` pass-through — zero re-validation cost. Re-validation only occurs for wrong-model `BaseModel` payloads or raw dicts (direct callers), where validation is the correct behavior. |
| Performance: `model_validate` on dict payload (direct callers only) | Low | `model_validate` is fast for small models — typically <10µs. Only reachable by direct callers, not the production dispatch path (which passes an already-validated `BaseModel`). |

## Downstream Consumer Impact Analysis

> **Note (M3):** The downstream repos (`~/src/warden`, `~/src/cennan`, `~/src/aacrtool`) are not available in the current environment. The "current pain point" descriptions below are **assumptions pending verification** against those repos. The breaking change impact is real regardless — `key_fn` signature changes from `dict[str, object]` to the validated model type.

### warden (`~/src/warden`) — Hybrid LLM proxy

**Current pain point (assumption):** Uses `KeyedRateLimitRef` for per-tenant, per-provider, per-model rate limiting. Payload models use `Field(alias="providerId")` and defaults. The untyped dict means `key_fn` has to know wire names (`p["providerId"]`) instead of model attributes (`p.provider_id`).

**Migration path:**
1. Change `KeyedRateLimitRef(base_name=..., key_fn=lambda p: p["providerId"], ...)` to `KeyedRateLimitRef.typed(WardenPayload, base_name=..., key_fn=lambda p: p.provider_id, ...)`
2. The `payload_type` field is set to `WardenPayload`, so the registry validates the payload and passes the model to `key_fn`
3. `key_fn` accesses `p.provider_id` (model attribute) instead of `p["providerId"]` (wire name)
4. Pydantic defaults (e.g., `region: str = "us-east-1"`) are applied automatically
5. Validation errors surface as `ValidationError` rather than `KeyError`

**Breaking change:** `key_fn` signature changes from `dict[str, object]` to the validated model type. Downstream code must update `key_fn` lambdas from `p["providerId"]` to `p.provider_id`. This is the correct, typesafe behavior the issue asked for.

### cennan (`~/src/cennan`) — Enterprise knowledge management

**Current pain point (assumption):** Uses rate limiting for ingestion API calls, keyed by tenant/KB. Has Pydantic models with defaults and aliases for binding configurations. `key_fn` must use wire names.

**Migration path:**
1. Change `KeyedRateLimitRef` and `KeyedReservationRef` constructions to use `.typed(CennanPayload, ...)`
2. `key_fn` accesses model attributes instead of dict keys
3. Defaults and aliases are applied by `model_validate`

**Breaking change:** `key_fn` signature changes from `dict[str, object]` to the validated model type. Downstream code must update `key_fn` lambdas from `p["tenantId"]` to `p.tenant_id`.

### aacrtool (`~/src/aacrtool`) — Agentic code review tool

**Current pain point (assumption):** Uses rate limiting for LLM API calls during reviews, keyed by provider/model. Has payload models with aliases for API compatibility.

**Migration path:**
1. Change `KeyedRateLimitRef` constructions to use `.typed(AacrtoolPayload, ...)`
2. `key_fn` accesses model attributes (`p.provider`, `p.model`) instead of wire names
3. Aliases are transparently applied

**Breaking change:** `key_fn` signature changes from `dict[str, object]` to the validated model type. Downstream code must update `key_fn` lambdas from `p["provider"]` to `p.provider`.

## Alignment with TaskQ Goals

| Goal | How this spec advances it |
|------|---------------------------|
| **Useful** | Solves a real pain point for all three downstream consumers (warden, cennan, aacrtool) who use aliases and defaults in their payload models. |
| **Flexible** | The registry accepts both dict and BaseModel payloads from direct callers, validating dicts against `ref.payload_type` — flexible for testing while always providing `key_fn` with the validated model. |
| **Powerful** | `key_fn` can now access computed properties, validators, and other Pydantic model features — not just raw dict keys. |
| **Typesafe** | The `.typed()` classmethod provides compile-time type checking of `key_fn` against the payload model. `p.tenant_id` is checked by pyright; `p["tenant_id"]` is not. The required `payload_type` field enables runtime `isinstance` checks to catch wrong-model mismatches. |
| **Robust** | Validation errors surface as `ValidationError` (a payload error) in the correct layer, not as `KeyError`/`TypeError` inside rate-limit resolution. Invalid payloads are rejected before a rate-limit token is consumed. |
| **Resilient** | The validated model is already constructed in the dispatch path; this spec reuses it rather than adding new validation cost. For the direct-call path, validation is the correct behavior (don't let a malformed payload consume a token). |
